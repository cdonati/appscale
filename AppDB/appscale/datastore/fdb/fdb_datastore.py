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
  ([namespace_dir], Greeting, 476633..., 155284..., 0) -> 0<protobuffer>

garbage: See the GarbageCollector class for more details.

indexes: This contains a directory for each index that the datastore needs in
order to satisfy basic queries along with indexes that the project has defined
for composite queries. Here is an example template:

  ([index_dir^3], <property-value>, [entity-path], <entity-version>) -> ''

transactions: This maps transaction handles to metadata that the datastore
needs in order to handle operations for the transaction. Here are a few example
entries:

  ([transactions_dir^4], <handle-id>, read_vs) -> <versionstamp>
  ([transactions_dir], <handle-id>, lookup, <op-id>) -> <entity-key>

versions: This maps commit versionstamps to entity versions. This mapping
is necessary because the API requires 64-bit values for entity versions, but
this implementation requires 80-bit versionstamps for determining if a
transaction can succeed. Here is the template along with an example key-value.

  ([namespace_dir^1], [entity-path], <versionstamp>) -> <entity-version>
  ([namespace_dir], Greeting, 476633..., <versionstamp>) -> 155284...

^1: A directory located at (appscale, datastore, <project>, <section>,
                            namespace)
^2: Items wrapped in "[]" represent multiple elements for brevity.
^3: The index's directory path. For example,
    (appscale, datastore, indexes, <namespace>, <kind>, <property_name>,
     <property_type>)
^4: A directory located at (appscale, datastore, <project>, transactions).
"""
import logging
import sys

from tornado import gen
from tornado.ioloop import IOLoop

from appscale.common.unpackaged import APPSCALE_PYTHON_APPSERVER
from appscale.datastore.dbconstants import BadRequest, InternalError
from appscale.datastore.fdb.garbage_collector import (
  GarbageCollector, PollingLock)
from appscale.datastore.fdb.utils import (
  DirectoryCache, EncodedTypes, flat_path, fdb, new_txid, next_entity_version,
  put_chunk, ScatteredAllocator, TornadoFDB)

sys.path.append(APPSCALE_PYTHON_APPSERVER)
from google.appengine.datastore import entity_pb

logger = logging.getLogger(__name__)


class FDBDatastore(object):
  """ A datastore implementation that uses FoundationDB. """

  # The Cloud Datastore API uses microseconds as version IDs. When the entity
  # doesn't exist, it reports the version as "1".
  _ABSENT_VERSION = 1

  _DATA_DIR = 'data'

  _ROOT_DIR = ('appscale', 'datastore')

  _TX_DIR = 'transactions'

  _VERSIONS_DIR = 'versions'

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

    if put_request.auto_id_policy() != put_request.CURRENT:
      raise BadRequest('Sequential allocator is not implemented')

    tr = self._db.create_transaction()

    if put_request.has_transaction():
      txid = put_request.transaction().handle()
      tx_dir = self._directory_cache.get((project_id, self._TX_DIR))

      encoded_entities = EncodedTypes.ENTITY_V3 + fdb.tuple.pack(
        tuple(entity.Encode() for entity in put_request.entity_list()))
      subspace = tx_dir.subspace((txid, 'puts'))
      put_chunk(tr, encoded_keys, subspace, add_vs=True)
      yield self._tornado_fdb.commit(tr)
      return

    futures = []
    for entity in put_request.entity_list():
      futures.append(self._upsert(tr, entity))

    writes = yield futures
    for key, old_version, new_version in writes:
      put_response.add_key().CopyFrom(key)
      put_response.add_version(new_version)

    yield self._tornado_fdb.commit(tr)

    logger.debug('put_response:\n{}'.format(put_response))

  @gen.coroutine
  def dynamic_get(self, project_id, get_request, get_response):
    logger.debug('get_request:\n{}'.format(get_request))

    tr = self._db.create_transaction()

    read_vs = None
    if get_request.has_transaction():
      txid = get_request.transaction().handle()
      tx_dir = self._directory_cache.get((project_id, self._TX_DIR))
      read_vs_key = tx_dir.pack((txid, 'read_vs'))
      read_vs = yield self._tornado_fdb.get(tr, read_vs_key)

      encoded_keys = EncodedTypes.KEY_V3 + fdb.tuple.pack(
        tuple(key.Encode() for key in get_request.key_list()))
      subspace = tx_dir.subspace((txid, 'reads'))
      put_chunk(tr, encoded_keys, subspace, add_vs=True)

    futures = []
    for key in get_request.key_list():
      futures.append(self._get(tr, key, read_vs))

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

    tr = self._db.create_transaction()

    if delete_request.has_transaction():
      txid = delete_request.transaction().handle()
      tx_dir = self._directory_cache.get((project_id, self._TX_DIR))

      encoded_keys = EncodedTypes.KEY_V3 + fdb.tuple.pack(
        tuple(key.Encode() for key in delete_request.key_list()))
      subspace = tx_dir.subspace((txid, 'deletes'))
      put_chunk(tr, encoded_keys, subspace, add_vs=True)
      yield self._tornado_fdb.commit(tr)
      return

    futures = []
    for key in delete_request.key_list():
      futures.append(self._delete(tr, key))

    deletes = yield futures

    yield self._tornado_fdb.commit(tr)

    # TODO: Once the Cassandra backend is removed, populate a delete response.
    for old_version, new_version in deletes:
      logger.debug('new_version: {}'.format(new_version))

  @gen.coroutine
  def setup_transaction(self, project_id, is_xg):
    txid = new_txid()
    tx_dir = self._directory_cache.get((project_id, self._TX_DIR))

    tr = self._db.create_transaction()
    read_vs_key = tx_dir.pack((txid, 'read_vs'))
    read_vs = yield self._tornado_fdb.get(tr, read_vs_key)
    if read_vs.present():
      raise InternalError('The datastore chose an existing txid')

    tr.set_versionstamped_value(read_vs_key, '\xff' * 14)
    tr[tx_dir.pack((txid, 'xg'))] = '1' if is_xg else '0'
    yield self._tornado_fdb.commit(tr)

    raise gen.Return(txid)

  @gen.coroutine
  def _upsert(self, tr, entity):
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
    encoded_value = EncodedTypes.ENTITY_V3 + encoded_entity
    old_entity, old_version = yield self._get_from_range(
      tr, data_ns_dir.range(path))

    # If the datastore chose an ID, don't overwrite existing data.
    if auto_id and old_version != self._ABSENT_VERSION:
      self._scattered_allocator.invalidate()
      raise InternalError('The datastore chose an existing ID')

    new_version = next_entity_version(old_version)
    subspace = data_ns_dir.pack(path + (new_version,))
    put_chunk(tr, encoded_value, subspace)

    # Map commit versionstamp to entity version.
    versions_dir = self._directory_cache.get((project_id, self._VERSIONS_DIR,
                                              namespace))
    key = versions_dir.pack_with_versionstamp(
      path + (fdb.tuple.Versionstamp(),))
    tr.set_versionstamped_key(key, new_version)

    raise gen.Return((entity.key(), old_version, new_version))

  @gen.coroutine
  def _get(self, tr, key, read_vs):
    path = flat_path(key)

    project_id = key.app()
    namespace = key.name_space()
    data_ns_dir = self._directory_cache.get((project_id, self._DATA_DIR,
                                             namespace))
    data_range = data_ns_dir.range(path)
    # Ignore values written after the start of the transaction.
    if read_vs is not None:

      data_range = slice(data_range.start, data_ns_dir.pack(path + (read_vs,)))

    entity, version = yield self._get_from_range(tr, data_range)
    raise gen.Return((key, entity, version))

  @gen.coroutine
  def _delete(self, tr, key):
    path = flat_path(key)

    project_id = key.app()
    namespace = key.name_space()
    data_ns_dir = self._directory_cache.get((project_id, self._DATA_DIR,
                                             namespace))

    old_entity, old_version = yield self._get_from_range(
      tr, data_ns_dir.range(path))

    if old_entity is None:
      raise gen.Return((old_version, old_version))

    new_version = next_entity_version(old_version)
    chunk_key = data_ns_dir.pack(path + (new_version, 0))
    tr[chunk_key] = ''

    # Map commit versionstamp to entity version.
    versions_dir = self._directory_cache.get((project_id, self._VERSIONS_DIR,
                                              namespace))
    key = versions_dir.pack_with_versionstamp(
      path + (fdb.tuple.Versionstamp(),))
    tr.set_versionstamped_key(key, new_version)

    raise gen.Return((old_version, new_version))

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

    if value[0] != EncodedTypes.ENTITY_V3:
      raise InternalError('Unknown entity type')

    entity = entity_pb.EntityProto(value[1:])
    raise gen.Return((entity, version))
