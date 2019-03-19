""" A datastore implementation that uses FoundationDB.

All datastore state is split between multiple FoundationDB directories. All of
the state for a given project is stored in (appscale, datastore, <project-id>).
Within each project directory, there is a directory for each of the following:

data: encoded entity data
indexes: entity key references by property values
transactions: transaction metadata

data: This maps entity keys to encoded entity data. The data is a tuple that
specifies the version and the encoding. Due to FDB's value size limit, data
that exceeds the chunk size threshold is split into multiple key-values. The
index value indicates the position of the chunk. Here is an example template
along with an example key-value:

  ([directory^1], [path]^2, <vs^3>, <index>) -> (<version>, <encoding>, <data>)
  ([directory], Greeting, 476633..., <vs>, 0) -> (0, 155284..., <protobuffer>)

indexes: This contains a directory for each index that the datastore needs in
order to satisfy basic queries along with indexes that the project has defined
for composite queries. Here is an example template:

  ([index dir^4], <property value>, [entity path], <commit versionstamp>) -> ''

transactions: This maps transaction handles to metadata that the datastore
needs in order to handle operations for the transaction. Here are a few example
entries:

  ([directory^5], <handle id>, read_vs) -> <read versionstamp^6>
  ([directory], <handle>, lookups, <rpc versionstamp>) -> (<encoding>, <key>)

^1: A directory located at (appscale, datastore, <project>, data, <namespace>).
^2: Items wrapped in "[]" represent multiple elements for brevity.
^3: A FoundationDB-generated value specifying the commit versionstamp. It is
    used for enforcing consistency.
^4: The index's directory path. For example,
    (appscale, datastore, <project>, indexes, <namespace>, <kind>,
     <property name>, <property type>)
^5: A directory located at (appscale, datastore, <project>, transactions).
^6: Designates what version of the database read operations should see.
"""
import logging
import sys

from tornado import gen
from tornado.ioloop import IOLoop

from appscale.common.unpackaged import APPSCALE_PYTHON_APPSERVER
from appscale.datastore.dbconstants import (
  BadRequest, ConcurrentModificationException, InternalError)
from appscale.datastore.fdb.utils import (
  decode_chunks, DirectoryCache, EncodedTypes, flat_path, fdb, log_request,
  new_txid, next_entity_version, put_chunks, ScatteredAllocator, TornadoFDB)

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

  def start(self):
    self._db = fdb.open()
    ds_dir = fdb.directory.create_or_open(self._db, self._ROOT_DIR)
    self._directory_cache = DirectoryCache(self._db, ds_dir)
    self._tornado_fdb = TornadoFDB(IOLoop.current())

  @gen.coroutine
  def dynamic_put(self, project_id, put_request, put_response):
    logger.debug('put_request:\n{}'.format(put_request))

    if put_request.auto_id_policy() != put_request.CURRENT:
      raise BadRequest('Sequential allocator is not implemented')

    tr = self._db.create_transaction()

    if put_request.has_transaction():
      tx_dir = self._directory_cache.get((project_id, self._TX_DIR))
      log_request(tr, tx_dir, put_request)
      writes = [(entity.key(), None, None)
                for entity in put_request.entity_list()]
    else:
      futures = []
      for entity in put_request.entity_list():
        futures.append(self._upsert(tr, entity))

      writes = yield futures

    yield self._tornado_fdb.commit(tr)

    for key, old_version, new_version in writes:
      put_response.add_key().CopyFrom(key)
      if new_version is not None:
        put_response.add_version(new_version)

    logger.debug('put_response:\n{}'.format(put_response))

  @gen.coroutine
  def dynamic_get(self, project_id, get_request, get_response):
    logger.debug('get_request:\n{}'.format(get_request))

    tr = self._db.create_transaction()

    read_vs = None
    if get_request.has_transaction():
      tx_dir = self._directory_cache.get((project_id, self._TX_DIR))
      vs_key = tx_dir.pack((get_request.transaction().handle(), 'read_vs'))
      read_vs = yield self._tornado_fdb.get(tr, vs_key)
      log_request(tr, tx_dir, get_request)

    futures = []
    for key in get_request.key_list():
      futures.append(self._get(tr, key, read_vs))

    results = yield futures
    for key, entity, version, _ in results:
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
      tx_dir = self._directory_cache.get((project_id, self._TX_DIR))
      log_request(tr, tx_dir, delete_request)
      deletes = [(key, None, None) for key in delete_request.key_list()]
    else:
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
  def apply_txn_changes(self, project_id, txid):
    tr = self._db.create_transaction()
    tx_dir = self._directory_cache.get((project_id, self._TX_DIR))
    read_vs, max_groups, lookups, mutations = yield self._get_tx_metadata(
      tr, tx_dir, txid)

    # Index keys that require a full lookup rather than a versionstamp.
    require_data = set()
    for mutation in mutations:
      key = (mutation if isinstance(mutation, entity_pb.Reference)
             else mutation.key())
      require_data.add(key.Encode())

    # Start fetching versionstamps for lookups first to invalidate sooner.
    futures = {}
    for key in lookups:
      encoded_key = key.Encode()
      futures[encoded_key] = self._get(
        tr, key, vs_only=(encoded_key in require_data))

    # Fetch remaining entities that were mutated.
    for mutation in mutations:
      key = (mutation if isinstance(mutation, entity_pb.Reference)
             else mutation.key())
      encoded_key = key.Encode()
      if encoded_key not in futures:
        futures[encoded_key] = self._get(tr, key)

    for key in lookups:
      commit_vs = yield futures[key.Encode()][-1]
      if commit_vs > read_vs:
        raise ConcurrentModificationException(
          'An entity was modified after this transaction was started.')

    # Apply mutations.
    for mutation in mutations:
      op = 'delete' if isinstance(mutation, entity_pb.Reference) else 'put'
      key = mutation if op == 'delete' else mutation.key()
      entity, old_version = yield futures[key.Encode()][1:3]
      new_version = next_entity_version(old_version)
      encoded_entity = entity.Encode() if op == 'put' else ''
      namespace = key.name_space()
      path = flat_path(key)
      data_ns_dir = self._directory_cache.get((project_id, self._DATA_DIR,
                                               namespace))
      encoded_value = fdb.tuple.pack(new_version, EncodedTypes.ENTITY_V3,
                                     encoded_entity)
      put_chunks(tr, encoded_value, data_ns_dir.pack(path), add_vs=True)

    yield self._tornado_fdb.commit(tr)

  @gen.coroutine
  def _upsert(self, tr, entity):
    last_element = entity.key().path().element(-1)
    auto_id = last_element.has_id() and last_element.id() == 0
    if auto_id:
      last_element.set_id(self._scattered_allocator.get_id())

    project_id = entity.key().app()
    namespace = entity.key().name_space()
    path = flat_path(entity.key())
    data_ns_dir = self._directory_cache.get((project_id, self._DATA_DIR,
                                             namespace))

    old_entity, old_version = yield self._get_from_range(
      tr, data_ns_dir.range(path))[:2]

    # If the datastore chose an ID, don't overwrite existing data.
    if auto_id and old_version != self._ABSENT_VERSION:
      self._scattered_allocator.invalidate()
      raise InternalError('The datastore chose an existing ID')

    new_version = next_entity_version(old_version)
    encoded_value = fdb.tuple.pack(new_version, EncodedTypes.ENTITY_V3,
                                   entity.Encode())
    put_chunks(tr, encoded_value, data_ns_dir.pack(path), add_vs=True)

    raise gen.Return((entity.key(), old_version, new_version))

  @gen.coroutine
  def _get(self, tr, key, read_vs=None, vs_only=False):
    path = flat_path(key)

    project_id = key.app()
    namespace = key.name_space()
    data_ns_dir = self._directory_cache.get((project_id, self._DATA_DIR,
                                             namespace))
    data_range = data_ns_dir.range(path)

    # Ignore values written after the start of the transaction.
    if read_vs is not None:
      data_range = slice(data_range.start, data_ns_dir.pack(path + (read_vs,)))

    if vs_only:
      commit_vs = yield self._get_from_range(tr, data_range, vs_only=True)
      raise gen.Return((key, commit_vs))
    else:
      entity, version, commit_vs = yield self._get_from_range(tr, data_range)
      raise gen.Return((key, entity, version, commit_vs))

  @gen.coroutine
  def _delete(self, tr, key):
    path = flat_path(key)

    project_id = key.app()
    namespace = key.name_space()
    data_ns_dir = self._directory_cache.get((project_id, self._DATA_DIR,
                                             namespace))

    old_entity, old_version = yield self._get_from_range(
      tr, data_ns_dir.range(path))[:2]

    if old_entity is None:
      raise gen.Return((old_version, old_version))

    new_version = next_entity_version(old_version)
    encoded_value = fdb.tuple.pack(new_version, EncodedTypes.ENTITY_V3, '')
    put_chunks(tr, encoded_value, data_ns_dir.pack(path), add_vs=True)

    raise gen.Return((old_version, new_version))

  @gen.coroutine
  def _get_from_range(self, tr, data_range, vs_only=False):
    entity = None
    version = self._ABSENT_VERSION
    commit_vs = 0

    # Select the latest entity version.
    kvs, count, more_results = yield self._tornado_fdb.get_range(
      tr, data_range, limit=1, reverse=True)

    if not count:
      raise gen.Return((entity, version, commit_vs))

    last_chunk = kvs[0]
    last_key_path = fdb.tuple.unpack(last_chunk.key)
    index = last_key_path[-1]
    commit_vs = last_key_path[-2]

    if vs_only:
      raise gen.Return(commit_vs)

    # If the entity is split into chunks, fetch the rest of the chunks.
    start_key = fdb.tuple.pack(last_key_path[:-1])
    end_key = last_chunk.key
    chunks = [last_chunk.value]
    iteration = 1
    while index > 0:
      remaining_range = slice(start_key, end_key)
      kvs, count, more_results = yield self._tornado_fdb.get_range(
        tr, remaining_range, iteration=iteration, reverse=True)
      if not count:
        raise InternalError('Incomplete entity record')

      chunks = [kv.value for kv in reversed(kvs)] + chunks
      end_key = kvs[-1].key
      iteration += 1
      index = fdb.tuple.unpack(end_key)[-1]

    version, encoding, encoded_entity = fdb.tuple.unpack(''.join(chunks))
    if not encoded_entity:
      raise gen.Return((entity, version, commit_vs))

    if encoding != EncodedTypes.ENTITY_V3:
      raise InternalError('Unknown entity type')

    entity = entity_pb.EntityProto(encoded_entity)
    raise gen.Return((entity, version, commit_vs))

  @gen.coroutine
  def _get_tx_metadata(self, tr, tx_dir, txid):
    metadata_range = tx_dir.range((txid,))

    read_vs = None
    xg = None
    lookups = set()
    mutations = []

    tmp_chunks = []
    tmp_rpc_vs = None
    tmp_rpc_type = None

    iteration = 1
    start_key = metadata_range.start
    end_key = metadata_range.stop
    while True:
      remaining_range = slice(start_key, end_key)
      kvs, count, more_results = yield self._tornado_fdb.get_range(
        tr, remaining_range, streaming_mode=fdb.StreamingMode.want_all,
        iteration=iteration)

      for index, kv in enumerate(kvs):
        if index == count - 1:
          end_key = fdb.KeySelector.first_greater_than(kv.key)

        key_parts = metadata_range.unpack(kv.key)
        if key_parts[0] == 'read_vs':
          read_vs = fdb.tuple.Versionstamp.from_bytes(kv.value)
          continue

        if key_parts[0] == 'xg':
          read_vs = True if kv.value == '1' else False
          continue

        rpc_type, rpc_vs = key_parts
        if rpc_vs == tmp_rpc_vs:
          tmp_chunks.append(kv.value)
          continue

        if tmp_rpc_type == 'lookups':
          lookups.update(decode_chunks(tmp_chunks, tmp_rpc_type))
        elif tmp_rpc_type in ('puts', 'deletes'):
          mutations.extend(decode_chunks(tmp_chunks, tmp_rpc_type))

        tmp_chunks = [kv.value]
        tmp_rpc_vs = rpc_vs
        tmp_rpc_type = rpc_type

      if not more_results:
        break

      iteration += 1

    if tmp_chunks and tmp_rpc_type == 'lookups':
      lookups.update(decode_chunks(tmp_chunks, tmp_rpc_type))
    elif tmp_chunks and tmp_rpc_type in ('puts', 'deletes'):
      mutations.extend(decode_chunks(tmp_chunks, tmp_rpc_type))

    if read_vs is None or xg is None:
      raise BadRequest('Transaction not found')

    raise gen.Return((read_vs, lookups, mutations))

  @gen.coroutine
  def _current_vs(self, tr, key):
    versionstamp = 0
    project_id = key.app()
    namespace = key.name_space()
    data_ns_dir = self._directory_cache.get((project_id, self._DATA_DIR,
                                             namespace))
    path = flat_path(key)
    data_range = data_ns_dir.range(path)

    kvs, count, more_results = yield self._tornado_fdb.get_range(
      tr, data_range, limit=1, reverse=True)

    if not count:
      raise gen.Return(versionstamp)

    versionstamp = fdb.tuple.unpack(kvs[0].key)[-2]
    raise gen.Return(versionstamp)
