""" A datastore implementation that uses FoundationDB.

All datastore state is split between multiple FoundationDB directories. All of
the state for a given project is stored in (appscale, datastore, <project-id>).
Within each project directory, there is a directory for each of the following:

data: encoded entity data
garbage: used by the garbage collector to manage deleted data
indexes: entity key references by property values
transactions: transaction metadata
versions: a mapping of fdb versionstamps to entity versions

data: This maps entity keys to encoded entity data. The data is prefixed by
a byte that indicates how it is encoded. Due to FDB's value size limit, data
that exceeds the chunk size threshold is split into multiple key-values. The
index value indicates the position of the chunk. Here is an example template
along with an example key-value:

  ([namespace_dir^1], [entity-path]^2, <version>, <index>) -> <encoding><data>
  ([namespace_dir^1], Guestbook, default, Greeting, 5, 1, 0) -> 0<protobuffer>

garbage: See the GarbageCollector class for more details.

indexes: This contains a directory for each index that the datastore needs in
order to satisfy basic queries along with indexes that the project has defined
for composite queries. Here is an example template:

  ([index_dir^3], <property-value>, [entity-path], <entity-version>) -> ''

transactions: This maps transaction handles to metadata that the datastore
needs in order to handle operations for the transaction. Here are a few example
entries:

  ([transactions_dir], <handle-id>, versionstamp) -> <versionstamp>
  ([transactions_dir], <handle-id>, lookup, <op-id>) -> <entity-key>

versions: This maps entity versions to FoundationDB versionstamps. This mapping
is necessary because the API requires 64-bit values for entity versions, but
this implementation requires 80-bit versionstamps for determining if a
transaction can succeed. Here is the template along with an example key-value.

  ([namespace_dir^1], [entity-path], <entity-version>) -> <versionstamp>
  ([journal_dir], Guestbook, default, Greeting, 5, 1) -> <versionstamp>

^1: A directory located at (appscale, datastore, <section>, namespace)
^2: Items wrapped in "[]" represent multiple elements for brevity.
^3: The index's directory path. For example,
    (appscale, datastore, indexes, <namespace>, <kind>, <property_name>,
     <property_type>)
"""
import logging
import sys
import uuid

from tornado import gen
from tornado.ioloop import IOLoop

from appscale.common.unpackaged import APPSCALE_PYTHON_APPSERVER
from appscale.datastore.dbconstants import BadRequest, InternalError
from appscale.datastore.fdb.garbage_collector import (
  GarbageCollector, PollingLock)
from appscale.datastore.fdb.utils import (
  DirectoryCache, EntityTypes, flat_path, fdb,
  next_entity_version, ScatteredAllocator, TornadoFDB)

sys.path.append(APPSCALE_PYTHON_APPSERVER)
from google.appengine.datastore import entity_pb

logger = logging.getLogger(__name__)


class FDBDatastore(object):
  """ A datastore implementation that uses FoundationDB. """

  # The max number of bytes for each chunk in an encoded entity.
  _CHUNK_SIZE = 10000

  # The Cloud Datastore API uses microseconds as versions. When the entity
  # doesn't exist, it reports the version as "1".
  _ABSENT_VERSION = 1

  _DATA_DIR = 'data'

  _ROOT_DIR = ('appscale', 'datastore')

  def __init__(self):
    self._db = None
    self._directory_cache = None
    self._scattered_allocator = ScatteredAllocator()
    self._tornado_fdb = None
    self._gc = None

  def start(self):
    self._db = fdb.open()
    ds_dir = fdb.directory.create_or_open(self._db, self._ROOT_DIR)
    self._directory_cache = DirectoryCache(self._db, ds_dir)
    self._tornado_fdb = TornadoFDB(IOLoop.current())
    gc_lock = PollingLock(
      self._db, self._tornado_fdb, ds_dir.pack((GarbageCollector.LOCK_KEY,)))
    gc_lock.start()

    self._gc = GarbageCollector(
      self._db, self._tornado_fdb, gc_lock, self._directory_cache)
    self._gc.start()

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

    # TODO: Once the Cassandra backend is removed, populate a delete response.
    deletes = yield futures
    for version in deletes:
      logger.debug('version: {}'.format(version))

  # @gen.coroutine
  # def setup_transaction(self, app_id, is_xg):
  #   txid = uuid.uuid4()
  #   tr = self._db.create_transaction()
  #   tr[]
  #   in_progress = self.transaction_manager.get_open_transactions(app_id)
  #   yield self.datastore_batch.start_transaction(
  #     app_id, txid, is_xg, in_progress)
  #   raise gen.Return(txid)

  @gen.coroutine
  def _upsert(self, entity):
    last_element = entity.key().path().element(-1)
    auto_id = last_element.has_id() and last_element.id() == 0
    if auto_id:
      last_element.set_id(self._scattered_allocator.get_id())

    project_id = entity.key().app()
    namespace = entity.key().name_space()
    data_ns_dir = self._directory_cache.get((project_id, self._DATA_DIR,
                                             namespace))
    path = flat_path(entity.key())

    encoded_entity = entity.Encode()
    encoded_value = EntityTypes.ENTITY_V3 + encoded_entity
    chunk_indexes = [(n, n + self._CHUNK_SIZE)
                     for n in xrange(0, len(encoded_value), self._CHUNK_SIZE)]

    tr = self._db.create_transaction()

    old_entity, old_version = yield self._get_from_range(
      tr, data_ns_dir.range(path))

    # If the datastore chose an ID, don't overwrite existing data.
    if auto_id and old_version != self._ABSENT_VERSION:
      self._scattered_allocator.invalidate()
      raise InternalError('The datastore chose an existing ID')

    new_version = next_entity_version(old_version)
    for start, end in chunk_indexes:
      chunk_key = data_ns_dir.pack(path + (new_version, start))
      tr[chunk_key] = encoded_value[start:end]

    delete_old_version = old_version != self._ABSENT_VERSION
    versionstamp_future = None
    if delete_old_version:
      self._gc.index_deleted_version(tr, project_id, namespace, path,
                                     old_version)
      versionstamp_future = tr.get_versionstamp()

    yield self._tornado_fdb.commit(tr)

    if delete_old_version:
      self._gc.clear_later(project_id, namespace, path, old_version, versionstamp_future)

    raise gen.Return((entity.key(), new_version))

  @gen.coroutine
  def _get(self, key):
    path = flat_path(key)

    namespace = (key.app(), key.name_space())
    data_dir = self._directory_cache.get(namespace + Directories.DATA)

    tr = self._db.create_transaction()
    entity, version = yield self._get_from_range(tr, data_dir.range(path))
    raise gen.Return((key, entity, version))

  @gen.coroutine
  def _delete(self, key):
    path = flat_path(key)

    namespace = (key.app(), key.name_space())
    data_dir = self._directory_cache.get(namespace + Directories.DATA)

    tr = self._db.create_transaction()

    old_entity, old_version = yield self._get_from_range(
      tr, data_dir.range(path))

    if old_entity is None:
      raise gen.Return(old_version)

    new_version = next_entity_version(old_version)
    chunk_key = data_dir.pack(path + (new_version, 0))
    tr[chunk_key] = ''

    self._gc.index_deleted_version(tr, namespace, path, old_version)
    versionstamp_future = tr.get_versionstamp()

    yield self._tornado_fdb.commit(tr)

    self._gc.clear_later(namespace, path, old_version, versionstamp_future)

    raise gen.Return(new_version)

  @gen.coroutine
  def _get_from_range(self, tr, data_range):
    entity = None
    version = self._ABSENT_VERSION

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
    iteration = 1
    while index > 0:
      remaining_range = slice(start_key, end_key)
      kvs, count, more_results = yield self._tornado_fdb.get_range(
        tr, remaining_range, iteration=iteration, reverse=True)
      if not count:
        raise InternalError('Incomplete entity record')

      value = ''.join([kv.value for kv in reversed(kvs)]) + value
      end_key = kvs[-1].key
      iteration += 1
      index = fdb.tuple.unpack(end_key)[-1]

    if not value:
      raise gen.Return((entity, version))

    if value[0] != EntityTypes.ENTITY_V3:
      raise InternalError('Unknown entity type')

    entity = entity_pb.EntityProto(value[1:])
    raise gen.Return((entity, version))
