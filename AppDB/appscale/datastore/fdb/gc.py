import logging
import six
import time
from collections import deque

import mmh3
from tornado import gen
from tornado.ioloop import IOLoop

from appscale.datastore.fdb.codecs import decode_str, encode_path
from appscale.datastore.fdb.polling_lock import PollingLock
from appscale.datastore.fdb.utils import fdb

logger = logging.getLogger(__name__)


def hash_tuple(value):
  hashable_value = u''.join([six.text_type(element) for element in value])
  val = mmh3.hash(hashable_value.encode('utf-8'), signed=False)
  byte_array = bytearray((val % 256,))
  return bytes(byte_array)


def group_hash(key):
  group_path = encode_path(key.path())[:2]
  return hash_tuple(group_path)


class GarbageCollector(object):
  DELETED_VERSIONS_DIR = u'deleted-versions'

  SAFE_READ_DIR = u'safe-read'

  _LOCK_KEY = u'gc-lock'

  def __init__(self, db, tornado_fdb, data_manager, index_manager,
               directory_cache):
    self._db = db
    self._queue = deque()
    self._tornado_fdb = tornado_fdb
    self._data_manager = data_manager
    self._index_manager = index_manager
    self._directory_cache = directory_cache
    lock_key = self._directory_cache.root.pack((self._LOCK_KEY,))
    self._lock = PollingLock(self._db, self._tornado_fdb, lock_key)

  def start(self):
    self._lock.start()
    IOLoop.current().spawn_callback(self._process_deferred_deletes)
    # IOLoop.current().spawn_callback(self._groom_projects)

  def clear_later(self, entities, new_vs):
    safe_time = time.time() + 60
    for old_entity, old_vs in entities:
      # TODO: Strip raw properties and enforce a max queue size to keep memory
      # usage reasonable.
      self._queue.append((safe_time, old_entity, old_vs, new_vs))

  @gen.coroutine
  def safe_read_vs(self, tr, key):
    project_id = decode_str(key.app())
    safe_read_dir = self._directory_cache.get((project_id, self.SAFE_READ_DIR))
    safe_read_key = safe_read_dir.rawPrefix + group_hash(key)
    vs = yield self._tornado_fdb.get(tr, safe_read_key, snapshot=True)
    if not vs.present():
      raise gen.Return(None)

    raise gen.Return(fdb.tuple.Versionstamp(vs.value))

  def index_deleted_versions(self, tr, entities):
    for old_entity, old_vs in entities:
      key = self._deleted_index_key(
        old_entity, old_vs, fdb.tuple.Versionstamp())
      tr.set_versionstamped_key(key, b'')

  def _deleted_index_key(self, old_entity, old_vs, commit_vs):
    project_id = decode_str(old_entity.key().app())
    namespace = decode_str(old_entity.key().name_space())
    path = encode_path(old_entity.key().path())
    deleted_dir = self._directory_cache.get(
      (project_id, self.DELETED_VERSIONS_DIR))
    if commit_vs.is_complete():
      pack = deleted_dir.pack
    else:
      pack = deleted_dir.pack_with_versionstamp

    # The entity path is prefixed with a hash in order to scatter the writes.
    return pack((hash_tuple(path), commit_vs, namespace, path, old_vs))

  @gen.coroutine
  def _process_deferred_deletes(self):
    while True:
      try:
        yield self._process_queue()
      except Exception:
        # TODO: Exponential backoff here.
        logger.exception(u'Unexpected error while processing GC queue')
        yield gen.sleep(1)
        continue

  @gen.coroutine
  def _process_queue(self):
    current_time = time.time()
    tx_deadline = current_time + 2.5
    tr = None
    while True:
      if not self._queue:
        if tr is not None:
          yield self._tornado_fdb.commit(tr)

        yield gen.sleep(61)
        break

      safe_time, old_entity, old_vs, new_vs = self._queue.popleft()
      if safe_time > current_time:
        self._queue.appendleft((safe_time, old_entity, old_vs, new_vs))
        if tr is not None:
          yield self._tornado_fdb.commit(tr)

        yield gen.sleep(safe_time - current_time + 1)
        break

      if tr is None:
        tr = self._db.create_transaction()

      self._data_manager.hard_delete(tr, old_entity.key(), old_vs)
      self._index_manager.hard_delete_entries(tr, old_entity, old_vs)
      del tr[self._deleted_index_key(old_entity, old_vs, new_vs)]

      # Keep track of safe versionstamps to invalidate stale txids.
      project_id = decode_str(old_entity.key().app())
      safe_read_dir = self._directory_cache.get(
        (project_id, self.SAFE_READ_DIR))
      safe_read_key = safe_read_dir.rawPrefix + group_hash(old_entity.key())
      tr.byte_max(safe_read_key, new_vs.tr_version)

      if time.time() > tx_deadline:
        yield self._tornado_fdb.commit(tr)
        break

  @gen.coroutine
  def _groom_projects(self):
    while True:
      try:
        yield self._lock.acquire()
        for project_id in self._directory_cache.root.list(self._db):
          yield self._groom_project(project_id)
      except Exception:
        logger.exception(u'Unexpected error while grooming projects')
        yield gen.sleep(10)

  @gen.coroutine
  def _newest_vs(self, project_id):
    tr = self._db.create_transaction()
    deleted_dir = self._directory_cache.get(
      (project_id, self.DELETED_VERSIONS_DIR))

    def future_for_range(byte_num):
      scatter_byte = bytes(bytearray([scatter_byte_num]))
      hash_range = deleted_dir.range((scatter_byte,))
      return self._tornado_fdb.get_range(tr, hash_range, limit=1, reverse=True,
                                         snapshot=True)

    # Fetch in 4 batches of 64.
    for quadrant in range(4):
      futures = []
      for scatter_byte_num in range(quadrant * 64, (quadrant + 1) * 64):
        scatter_byte = bytes(bytearray([scatter_byte_num]))
        hash_range = deleted_dir.range((scatter_byte,))
        futures.append()
        if commit_vs.is_complete():
          pack = deleted_dir.pack
        else:
          pack = deleted_dir.pack_with_versionstamp

        # The entity path is prefixed with a hash in order to scatter the writes.
        return pack((hash_tuple(path), commit_vs, namespace, path, old_vs))
        scatter_byte =
    futures = []
    for scatter_byte in range(64)
