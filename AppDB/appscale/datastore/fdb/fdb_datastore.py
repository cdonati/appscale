""" A datastore implementation that uses FoundationDB.

All datastore state is split between multiple FoundationDB directories. All of
the state for a given project/namespace is stored in
(appscale, datastore, <project-id>, <namespace>). Within
each namespace directory, there is a directory for each of the following:

journal: This maps entity versions to FoundationDB versionstamps. This mapping
is necessary because the API requires 64-bit values for entity versions, but
this implementation requires 80-bit versionstamps for determining if a
transaction can succeed. Here is the template along with an example key-value.
Items wrapped in "[]" represent multiple elements for brevity.

  ([journal_dir], [entity-path], <entity-version>) -> <versionstamp>
  ([journal_dir], Guestbook, default, Greeting, 5, 1) -> <versionstamp>

entities: This maps entity keys to encoded entity data. The data is prefixed by
a byte that indicates how it is encoded. Due to FDB's value size limit, data
that exceeds the chunk size threshold is split into multiple key-values. The
index value indicates the position of the chunk. Here is the template along
with an example key-value:

  ([entities_dir], [entity-path], <version>, <index>) -> <encoding><data>
  ([entities_dir], Guestbook, default, Greeting, 5, 1, 0) -> 0<protobuffer>

indexes...

When an entity is updated, non-ancestor indexes from older versions are erased
immediately, but older entity versions are kept for at least the maximum
transaction duration to allow a transaction to see a consistent snapshot.

The first byte of an entity value indicates the type of object that is stored.

"""
import logging
import sys

from tornado import gen

from tornado.ioloop import IOLoop

from appscale.common.unpackaged import APPSCALE_PYTHON_APPSERVER
from appscale.datastore.dbconstants import BadRequest, InternalError
from appscale.datastore.fdb.utils import (
  DirectoryCache, EntityTypes, flat_path, fdb, ScatteredAllocator, TornadoFDB)

sys.path.append(APPSCALE_PYTHON_APPSERVER)
from google.appengine.datastore import entity_pb

logger = logging.getLogger(__name__)


class FDBDatastore(object):
  """ A datastore implementation that uses FoundationDB. """

  # The max number of bytes for each chunk in an encoded entity.
  _CHUNK_SIZE = 10000

  def __init__(self):
    self._db = None
    self._directory_cache = DirectoryCache()
    self._scattered_allocator = ScatteredAllocator()
    self._tornado_fdb = None

  def start(self):
    self._db = fdb.open()
    self._directory_cache.start(self._db)
    self._tornado_fdb = TornadoFDB(IOLoop.current())

  @gen.coroutine
  def dynamic_put(self, project_id, put_request, put_response):
    logger.debug('put_request:\n{}'.format(put_request))

    if put_request.has_transaction():
      raise BadRequest('Transactions are not implemented')

    if put_request.auto_id_policy() != put_request.CURRENT:
      raise BadRequest('Sequential allocator is not implemented')

    futures = []
    for entity in put_request.entity_list():
      if entity.key().app() != project_id:
        raise BadRequest('Project ID mismatch: '
                         '{} != {}'.format(entity.key().app(), project_id))

      futures.append(self._upsert(entity))

    writes = yield futures
    for key, version in writes:
      put_response.add_key().CopyFrom(key)
      put_response.add_version(version)

    logger.debug('put_response:\n{}'.format(put_response))

  @gen.coroutine
  def dynamic_get(self, project_id, get_request, get_response):
    logger.debug('get_request:\n{}'.format(get_request))

    if get_request.has_transaction():
      raise BadRequest('Transactions are not implemented')

    futures = []
    for key in get_request.key_list():
      if key.app() != project_id:
        raise BadRequest('Project ID mismatch: '
                         '{} != {}'.format(key.app(), project_id))

      futures.append(self._get(key))

    results = yield futures
    for key, entity, version in results:
      response_entity = get_response.add_entity()
      response_entity.mutable_key().CopyFrom(key)
      response_entity.set_version(version)
      if entity is not None:
        response_entity.mutable_entity().CopyFrom(entity)

    logger.debug('get_response:\n{}'.format(get_response))

  @gen.coroutine
  def dynamic_delete(self, project_id, delete_request):
    logger.debug('delete_request:\n{}'.format(delete_request))

    if delete_request.has_transaction():
      raise BadRequest('Transactions are not implemented')

    futures = []
    for key in delete_request.key_list():
      if key.app() != project_id:
        raise BadRequest('Project ID mismatch: '
                         '{} != {}'.format(key.app(), project_id))

      futures.append(self._delete(key))

    deletes = yield futures
    for version in deletes:
      delete_request.add_version(version)

    logger.debug('delete_response:\n{}'.format(delete_response))

  @gen.coroutine
  def _upsert(self, entity):
    path = flat_path(entity.key())

    auto_id = path[-1] == 0
    if auto_id:
      path[-1] = self._scattered_allocator.get_id()

    namespace = (entity.key().app(), entity.key().name_space())
    journal_dir = self._directory_cache.get(namespace + ('journal',))
    data_dir = self._directory_cache.get(namespace + ('data',))

    encoded_entity = entity.Encode()
    encoded_value = EntityTypes.ENTITY_V3 + encoded_entity
    chunk_indexes = [(n, n + self._CHUNK_SIZE)
                     for n in xrange(0, len(encoded_value), self._CHUNK_SIZE)]

    tr = self._db.create_transaction()

    old_entity, old_version = yield self._get_from_range(
      tr, data_dir.range(tuple(path)))

    # If the datastore chose an ID, don't overwrite existing data.
    if auto_id and old_version != 0:
      self._scattered_allocator.invalidate()
      raise InternalError('The datastore chose an existing ID')

    new_version = old_version + 1
    for start, end in chunk_indexes:
      chunk_key = data_dir.pack(tuple(path + [new_version, start]))
      tr[chunk_key] = encoded_value[start:end]

    journal_key = journal_dir.pack(tuple(path + [new_version]))
    tr.set_versionstamped_value(journal_key, '\x00' * 14)

    yield self._tornado_fdb.commit(tr)

    new_key = entity_pb.Reference()
    new_key.CopyFrom(entity.key())
    if auto_id:
      new_key.path().element_list()[-1].set_id(path[-1])

    raise gen.Return((new_key, new_version))

  @gen.coroutine
  def _get(self, key):
    path = flat_path(key)

    namespace = (key.app(), key.name_space())
    data_dir = self._directory_cache.get(namespace + ('data',))

    tr = self._db.create_transaction()

    entity, version = yield self._get_from_range(
      tr, data_dir.range(tuple(path)))

    raise gen.Return((key, entity, version))

  @gen.coroutine
  def _delete(self, key):
    path = flat_path(key)

    namespace = (key.app(), key.name_space())
    journal_dir = self._directory_cache.get(namespace + ('journal',))
    data_dir = self._directory_cache.get(namespace + ('data',))

    tr = self._db.create_transaction()

    old_entity, old_version = yield self._get_from_range(
      tr, data_dir.range(tuple(path)))

    if old_version == 0:
      raise gen.Return(old_version)

    new_version = old_version + 1

    chunk_key = data_dir.pack(tuple(path + [new_version, 0]))
    tr[chunk_key] = ''

    journal_key = journal_dir.pack(tuple(path + [new_version]))
    tr.set_versionstamped_value(journal_key, '\x00' * 14)

    yield self._tornado_fdb.commit(tr)

    raise gen.Return(new_version)

  @gen.coroutine
  def _get_from_range(self, tr, data_range):
    entity = None
    version = 0

    # Select the latest entity version.
    kvs, count, more_results = yield self._tornado_fdb.get_range(
      tr, data_range, limit=1, reverse=True)

    if not count:
      raise gen.Return((entity, version))

    last_chunk = kvs[0]
    last_key_path = fdb.tuple.unpack(last_chunk.key)
    index = last_key_path[-1]
    version = last_key_path[-2]

    # If the entity is split into chunks, fetch the rest of the chunks.
    start_key = fdb.tuple.pack(last_key_path[:-1])
    end_key = last_chunk.key
    value = last_chunk.value
    while index > 0:
      remaining_range = slice(start_key, end_key)
      kvs, count, more_results = yield self._tornado_fdb.get_range(
        tr, remaining_range, reverse=True)
      if not count:
        raise InternalError('Incomplete entity record')

      value = ''.join([kv.value for kv in reversed(kvs)]) + value
      end_key = kvs[-1].key
      index = fdb.tuple.unpack(end_key)[-1]

    if not value:
      raise gen.Return((entity, version))

    if value[0] != EntityTypes.ENTITY_V3:
      raise InternalError('Unknown entity type')

    entity = entity_pb.EntityProto(value[1:])
    raise gen.Return((entity, version))
