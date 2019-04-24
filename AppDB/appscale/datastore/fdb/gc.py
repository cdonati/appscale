import logging
import time
from collections import deque

import mmh3
import six
import six.moves as sm
from tornado import gen
from tornado.ioloop import IOLoop

from appscale.datastore.dbconstants import MAX_TX_DURATION
from appscale.datastore.fdb.codecs import decode_str, encode_path
from appscale.datastore.fdb.polling_lock import PollingLock
from appscale.datastore.fdb.utils import fdb, KVIterator

logger = logging.getLogger(__name__)


def hash_tuple(value):
  hashable_value = u''.join([six.text_type(element) for element in value])
  val = mmh3.hash(hashable_value.encode('utf-8'), signed=False)
  byte_array = bytearray((val % 256,))
  return bytes(byte_array)


def group_hash(key):
  group_path = encode_path(key.path())[:2]
  return hash_tuple(group_path)


class DeletedVersionEntry(object):
  __SLOTS__ = [u'project_id', u'namespace', u'path', u'original_vs',
               u'deleted_vs']

  def __init__(self, project_id, namespace, path, original_vs, deleted_vs):
    self.project_id = project_id
    self.namespace = namespace
    self.path = path
    self.original_vs = original_vs
    self.deleted_vs = deleted_vs


class DeletedVersionIndex(object):
  DIR_NAME = u'deleted-versions'

  def __init__(self, directory):
    self.directory = directory

  @classmethod
  def from_cache(cls, project_id, directory_cache):
    directory = directory_cache.get((project_id, cls.DIR_NAME))
    return cls(directory)

  @property
  def project_id(self):
    return self.directory.get_path()[2]

  def encode(self, entity, original_vs, deleted_vs):
    namespace = decode_str(entity.key().name_space())
    path = encode_path(entity.key().path())
    if deleted_vs.is_complete():
      pack = self.directory.pack
    else:
      pack = self.directory.pack_with_versionstamp

    # The entity path is prefixed with a hash in order to scatter the writes.
    return pack((hash_tuple(path), deleted_vs, namespace, path, original_vs))

  def decode(self, kv):
    _, deleted_vs, namespace, path, original_vs = self.directory.unpack(kv.key)
    return DeletedVersionEntry(self.project_id, namespace, path, original_vs,
                               deleted_vs)


class GarbageCollector(object):
  SAFE_READ_DIR = u'safe-read'

  _LOCK_KEY = u'gc-lock'

  # The number of extra seconds to wait before checking which versions are safe
  # to delete. A larger value results in fewer GC transactions. It also results
  # in a more relaxed max transaction duration.
  _DEFERRED_DEL_PADDING = 2

  # Give the deferred deletion process a chance to succeed before grooming.
  _SAFETY_INTERVAL = MAX_TX_DURATION * 2

  # The percantage of scattered index space to groom at a time. There is no
  # urgency. This fraction's reciprocal should be a factor of 256.
  _BATCH_PERCENT = .125

  # The number of ranges to groom within a single transaction.
  _BATCH_COUNT = int(_BATCH_PERCENT * 256)

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
    IOLoop.current().spawn_callback(self._groom_projects)

  def clear_later(self, entities, new_vs):
    safe_time = time.time() + MAX_TX_DURATION
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

  def index_deleted_versions(self, tr, project_id, entities):
    index = DeletedVersionIndex.from_cache(project_id, self._directory_cache)
    for old_entity, original_vs in entities:
      key = index.encode(old_entity, original_vs, fdb.tuple.Versionstamp())
      tr.set_versionstamped_key(key, b'')

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
      safe_time = next(iter(self._queue), [current_time + MAX_TX_DURATION])[0]
      if current_time < safe_time:
        if tr is not None:
          yield self._tornado_fdb.commit(tr)

        yield gen.sleep(safe_time - current_time + self._DEFERRED_DEL_PADDING)
        break

      safe_time, old_entity, original_vs, deleted_vs = self._queue.popleft()
      project_id = decode_str(old_entity.key().app())
      if tr is None:
        tr = self._db.create_transaction()

      self._data_manager.hard_delete(tr, old_entity.key(), original_vs)
      self._index_manager.hard_delete_entries(tr, old_entity, original_vs)
      index = DeletedVersionIndex.from_cache(project_id, self._directory_cache)
      del tr[index.encode(old_entity, original_vs, deleted_vs)]

      # Keep track of safe versionstamps to invalidate stale txids.
      safe_read_dir = self._directory_cache.get(
        (project_id, self.SAFE_READ_DIR))
      safe_read_key = safe_read_dir.rawPrefix + group_hash(old_entity.key())
      tr.byte_max(safe_read_key, deleted_vs.tr_version)

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
  def _groom_project(self, project_id):
    for batch_num in sm.range(int(1 / self._BATCH_PERCENT)):
      ranges = sm.range(batch_num * self._BATCH_COUNT,
                        (batch_num + 1) * self._BATCH_COUNT)
      safe_vs = yield self._newest_vs(project_id, ranges)
      yield gen.sleep(self._SAFETY_INTERVAL)
      if safe_vs is not None:
        yield self._groom_ranges(project_id, safe_vs, ranges)

  @gen.coroutine
  def _newest_vs(self, project_id, ranges):
    yield self._lock.acquire()
    tr = self._db.create_transaction()
    index = DeletedVersionIndex.from_cache(project_id, self._directory_cache)
    def newest_from_range(byte_num):
      scatter_byte = bytes(bytearray([byte_num]))
      hash_range = index.directory.range((scatter_byte,))
      return self._tornado_fdb.get_range(tr, hash_range, limit=1, reverse=True,
                                         snapshot=True)

    all_kvs = yield [newest_from_range(range_) for range_ in ranges]
    newest_vs = max(
      [index.decode(kvs[0]).deleted_vs for kvs in all_kvs if kvs] or [None])

    raise gen.Return(newest_vs)

  def _groom_ranges(self, project_id, safe_vs, ranges):
    yield self._lock.acquire()
    tr = self._db.create_transaction()
    index = DeletedVersionIndex.from_cache(project_id, self._directory_cache)
    def iter_for_byte(byte_num):
      scatter_byte = bytes(bytearray([byte_num]))
      safe_start = index.directory.range((scatter_byte,)).start
      safe_stop = fdb.KeySelector.first_greater_than(
        index.directory.pack((scatter_byte, safe_vs)))
      safe_range = slice(safe_start, safe_stop)
      return KVIterator(tr, self._tornado_fdb, safe_range)

    iterators = [iter_for_byte(range_) for range_ in ranges]
    responses = yield [iterator.next_page() for iterator in iterators]
    for kvs, more in responses:

      if kvs:
        oldest_vs = min(deleted_dir.unpack(kvs[0].key)[1], oldest_vs)
