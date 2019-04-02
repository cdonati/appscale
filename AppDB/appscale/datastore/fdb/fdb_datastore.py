""" A datastore implementation that uses FoundationDB.

All datastore state is split between multiple FoundationDB directories. All of
the state for a given project is stored in (appscale, datastore, <project-id>).
Within each project directory, there is a directory for each of the following:

data: encoded entity data
indexes: entity key references by property values
transactions: transaction metadata

"""
import logging
import sys

import six
from tornado import gen
from tornado.ioloop import IOLoop

from appscale.common.unpackaged import APPSCALE_PYTHON_APPSERVER
from appscale.datastore.dbconstants import (
  BadRequest, ConcurrentModificationException, InternalError)
from appscale.datastore.fdb.data import DataManager
from appscale.datastore.fdb.indexes import IndexManager
from appscale.datastore.fdb.transactions import TransactionManager
from appscale.datastore.fdb.utils import (
  ABSENT_VERSION, DirectoryCache, fdb, next_entity_version, ScatteredAllocator,
  TornadoFDB)

sys.path.append(APPSCALE_PYTHON_APPSERVER)
from google.appengine.datastore import entity_pb

logger = logging.getLogger(__name__)


class FDBDatastore(object):
  """ A datastore implementation that uses FoundationDB. """

  _ROOT_DIR = (u'appscale', u'datastore')

  def __init__(self):
    self.index_manager = None
    self._data_manager = None
    self._db = None
    self._directory_cache = None
    self._scattered_allocator = ScatteredAllocator()
    self._tornado_fdb = None
    self._tx_manager = None

  def start(self):
    self._db = fdb.open()
    ds_dir = fdb.directory.create_or_open(self._db, self._ROOT_DIR)
    self._directory_cache = DirectoryCache(self._db, ds_dir)
    self._tornado_fdb = TornadoFDB(IOLoop.current())
    self._data_manager = DataManager(self._directory_cache, self._tornado_fdb)
    self.index_manager = IndexManager(self._db, self._directory_cache,
                                      self._tornado_fdb, self._data_manager)
    self._tx_manager = TransactionManager(self._directory_cache,
                                          self._tornado_fdb)

  @gen.coroutine
  def dynamic_put(self, project_id, put_request, put_response):
    logger.debug('put_request:\n{}'.format(put_request))
    project_id = six.text_type(project_id)

    if put_request.auto_id_policy() != put_request.CURRENT:
      raise BadRequest('Sequential allocator is not implemented')

    tr = self._db.create_transaction()

    if put_request.has_transaction():
      self._tx_manager.log_rpc(tr, project_id, put_request)
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
    project_id = six.text_type(project_id)
    tr = self._db.create_transaction()

    read_vs = None
    if get_request.has_transaction():
      read_vs = yield self._tx_manager.get_read_vs(
        tr, project_id, get_request.transaction().handle())
      self._tx_manager.log_rpc(tr, project_id, get_request)

    futures = []
    for key in get_request.key_list():
      futures.append(self._data_manager.get_latest(tr, key, read_vs))

    results = yield futures
    for key, encoded_entity, version, _ in results:
      response_entity = get_response.add_entity()
      response_entity.mutable_key().CopyFrom(key)
      response_entity.set_version(version)
      if encoded_entity:
        entity = entity_pb.EntityProto(encoded_entity)
        response_entity.mutable_entity().CopyFrom(entity)

    logger.debug('get_response:\n{}'.format(get_response))

  @gen.coroutine
  def dynamic_delete(self, project_id, delete_request):
    logger.debug('delete_request:\n{}'.format(delete_request))
    project_id = six.text_type(project_id)
    tr = self._db.create_transaction()

    if delete_request.has_transaction():
      self._tx_manager.log_rpc(tr, project_id, delete_request)
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
  def _dynamic_run_query(self, query, query_result):
    logger.debug('query: {}'.format(query))
    project_id = six.text_type(query.app())
    tr = self._db.create_transaction()
    read_vs = None
    if query.has_transaction():
      read_vs = yield self._tx_manager.get_read_vs(
        tr, project_id, query.transaction().handle())
      self._tx_manager.log_query(tr, project_id, query)

    fetch_data = self.index_manager.include_data(query)
    rpc_limit, check_more_results = self.index_manager.rpc_limit(query)

    logger.debug('about to get iterator')
    iterator = self.index_manager.get_iterator(tr, query, read_vs)
    logger.debug('iterator: {}'.format(iterator))
    for prop_name in query.property_name_list():
      prop_name = six.text_type(prop_name)
      if prop_name not in iterator.prop_names:
        raise BadRequest('Projections on {} are not '
                         'supported'.format(prop_name))

    data_futures = [] if fetch_data else None
    unique_keys = set()
    results = []
    entries_fetched = 0
    skipped_results = 0
    cursor = None
    while True:
      remainder = rpc_limit - entries_fetched
      iter_offset = max(query.offset() - entries_fetched, 0)
      entries, more_iterator_results = yield iterator.next_page()
      logger.debug('entries: {}'.format(entries))
      logger.debug('more_iterator_results: {}'.format(more_iterator_results))
      entries_fetched += len(entries)
      if not entries and more_iterator_results:
        continue

      if not entries and not more_iterator_results:
        break

      skipped_results += min(len(entries), iter_offset)
      suitable_entries = entries[iter_offset:remainder]
      cursor = entries[:remainder][-1]

      if not fetch_data and not query.keys_only():
        results.extend([entry.prop_result() for entry in suitable_entries])
        continue

      for entry in suitable_entries:
        if entry.path in unique_keys:
          continue

        unique_keys.add(entry.path)
        if fetch_data:
          data_futures.append(
            self._data_manager.get_entry(tr, entry, snapshot=True))
        else:
          results.append(entry.key_result())

      if not more_iterator_results:
        break

    if fetch_data:
      entity_results = yield data_futures
      results = [encoded_entity for version, encoded_entity in entity_results]
    else:
      results = [result.Encode() for result in results]

    query_result.result_list().extend(results)
    if query.compile():
      mutable_cursor = query_result.mutable_compiled_cursor()
      if cursor is not None:
        mutable_cursor.MergeFrom(cursor.cursor_result())

    more_results = check_more_results and entries_fetched > rpc_limit
    query_result.set_more_results(more_results)

    if skipped_results:
      query_result.set_skipped_results(skipped_results)

    if query.keys_only():
      query_result.set_keys_only(True)

    logger.debug('query_result: {}'.format(query_result))
    for encoded_entity in query_result.result_list():
      logger.debug('entity: {}'.format(entity_pb.EntityProto(encoded_entity)))

  @gen.coroutine
  def setup_transaction(self, project_id, is_xg):
    project_id = six.text_type(project_id)
    tr = self._db.create_transaction()
    txid = yield self._tx_manager.create(tr, project_id, is_xg)
    yield self._tornado_fdb.commit(tr)
    logger.debug('created txid: {}'.format(txid))
    raise gen.Return(txid)

  @gen.coroutine
  def apply_txn_changes(self, project_id, txid):
    project_id = six.text_type(project_id)
    tr = self._db.create_transaction()
    tx_metadata = yield self._tx_manager.get_metadata(tr, project_id, txid)
    read_vs, xg, lookups, queried_groups, mutations = tx_metadata

    group_update_futures = [
      self._data_manager.last_commit(tr, project_id, namespace, group_path)
      for namespace, group_path in queried_groups]

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
      if encoded_key in require_data:
        futures[encoded_key] = self._data_manager.get_latest(tr, key)
      else:
        futures[encoded_key] = self._data_manager.latest_vs(tr, key)

    # Fetch remaining entities that were mutated.
    for mutation in mutations:
      key = (mutation if isinstance(mutation, entity_pb.Reference)
             else mutation.key())
      encoded_key = key.Encode()
      if encoded_key not in futures:
        futures[encoded_key] = self._data_manager.get_latest(tr, key)

    group_updates = yield group_update_futures
    if any(commit_vs > read_vs for commit_vs in group_updates):
      raise ConcurrentModificationException(
        'A queried group was modified after this transaction was started.')

    for key in lookups:
      latest_commit_vs = yield futures[key.Encode()]
      if isinstance(latest_commit_vs, tuple):
        latest_commit_vs = latest_commit_vs[-1]

      if latest_commit_vs > read_vs:
        raise ConcurrentModificationException(
          'An entity was modified after this transaction was started.')

    # Apply mutations.
    for mutation in mutations:
      op = 'delete' if isinstance(mutation, entity_pb.Reference) else 'put'
      key = mutation if op == 'delete' else mutation.key()
      old_entities = yield futures[key.Encode()]
      old_encoded, old_version, old_vs = old_entities[1:]
      if old_encoded:
        old_entity = entity_pb.EntityProto(old_encoded)
      else:
        old_entity = None

      new_version = next_entity_version(old_version)
      new_encoded = mutation.Encode() if op == 'put' else ''
      self._data_manager.put(tr, key, new_version, new_encoded)
      new_entity = mutation if op == 'put' else None
      self.index_manager.put_entries(tr, old_entity, old_vs, new_entity)

    yield self._tornado_fdb.commit(tr)

  @gen.coroutine
  def rollback_transaction(self, project_id, txid):
    project_id = six.text_type(project_id)
    logger.info(
      u'Doing a rollback on transaction {} for {}'.format(txid, project_id))

    tr = self._db.create_transaction()
    self._tx_manager.delete(tr, project_id, txid)
    yield self._tornado_fdb.commit(tr)

  @gen.coroutine
  def update_composite_index(self, project_id, index):
    project_id = six.text_type(project_id)
    yield self.index_manager.update_composite_index(project_id, index)

  @gen.coroutine
  def _upsert(self, tr, entity):
    last_element = entity.key().path().element(-1)
    auto_id = last_element.has_id() and last_element.id() == 0
    if auto_id:
      last_element.set_id(self._scattered_allocator.get_id())

    _, old_encoded, old_version, old_vs = yield self._data_manager.get_latest(
      tr, entity.key())
    if old_encoded:
      old_entity = entity_pb.EntityProto(old_encoded)
    else:
      old_entity = None

    # If the datastore chose an ID, don't overwrite existing data.
    if auto_id and old_version != ABSENT_VERSION:
      self._scattered_allocator.invalidate()
      raise InternalError('The datastore chose an existing ID')

    new_version = next_entity_version(old_version)
    self._data_manager.put(tr, entity.key(), new_version, entity.Encode())
    self.index_manager.put_entries(tr, old_entity, old_vs, entity)

    raise gen.Return((entity.key(), old_version, new_version))

  @gen.coroutine
  def _delete(self, tr, key):
    _, old_encoded, old_version, old_vs = yield self._data_manager.get_latest(
      tr, key)

    if old_encoded is None:
      raise gen.Return((old_version, old_version))

    old_entity = entity_pb.EntityProto(old_encoded)
    new_version = next_entity_version(old_version)
    self._data_manager.put(tr, key, new_version, '')
    self.index_manager.put_entries(tr, old_entity, old_vs, new_entity=None)

    raise gen.Return((old_version, new_version))
