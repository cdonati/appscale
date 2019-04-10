import logging
import six
import time
from collections import deque

import mmh3
from tornado import gen
from tornado.ioloop import IOLoop

from appscale.datastore.fdb.codecs import decode_str, encode_path
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


def path_hash(key):
  path = encode_path(key.path())
  return hash_tuple(path)


class GarbageCollector(object):
  DELETED_VERSIONS_DIR = u'deleted-versions'

  SAFE_READ_DIR = u'safe-read'

  def __init__(self, db, tornado_fdb, data_manager, index_manager,
               directory_cache):
    self._db = db
    self._queue = deque()
    self._tornado_fdb = tornado_fdb
    self._data_manager = data_manager
    self._index_manager = index_manager
    self._directory_cache = directory_cache

  def start(self):
    IOLoop.current().spawn_callback(self._run_forever)

  def clear_later(self, entities, new_vs):
    safe_time = time.time() + 60
    for old_entity, old_vs in entities:
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
    op_id = 0
    for old_entity, old_vs in entities:
      project_id = decode_str(old_entity.key().app())
      namespace = decode_str(old_entity.key().name_space())
      encoded_path = encode_path(old_entity.key().path())
      deleted_dir = self._directory_cache.get(
        (project_id, self.DELETED_VERSIONS_DIR))
      # The entity path is hashed in order to scatter the writes.
      key = deleted_dir.pack_with_versionstamp(
        (path_hash(old_entity.key()), fdb.tuple.Versionstamp(), op_id))
      value = fdb.tuple.pack(
        (project_id, namespace) + encoded_path + (old_vs,))
      tr.set_versionstamped_key(key, value)
      op_id += 1

  def _deleted_index_key(self, entity):
    project_id = decode_str(entity.key().app())
    namespace = decode_str(entity.key().name_space())

  @gen.coroutine
  def _run_forever(self):
    while True:
      try:
        yield self._process_queue()
      except Exception:
        # TODO: Exponential backoff here.
        logger.exception('Unexpected error while processing GC queue')
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

      # Keep track of the newest version in the group that was deleted.
      project_id = decode_str(old_entity.key().app())
      safe_read_dir = self._directory_cache.get(
        (project_id, self.SAFE_READ_DIR))
      safe_read_key = safe_read_dir.rawPrefix + group_hash(old_entity.key())
      tr.byte_max(safe_read_key, new_vs.tr_version)

      if time.time() > tx_deadline:
        yield self._tornado_fdb.commit(tr)
        break
