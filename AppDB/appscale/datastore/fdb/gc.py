"""
This module keeps track of outdated transactions and entity versions and
deletes them when sufficient time has passed. The GarbageCollector is the main
interface that clients can use to mark resources as old and determine if a
resource is still available.
"""
import logging
import monotonic
from collections import deque

import six.moves as sm
from tornado import gen
from tornado.ioloop import IOLoop

from appscale.datastore.dbconstants import MAX_TX_DURATION
from appscale.datastore.fdb.cache import NSCache
from appscale.datastore.fdb.codecs import (
  decode_str, encode_path, encode_vs_index)
from appscale.datastore.fdb.polling_lock import PollingLock
from appscale.datastore.fdb.utils import (
  DS_ROOT, fdb, hash_tuple, KVIterator, VS_SIZE)

logger = logging.getLogger(__name__)


class DeletedVersionEntry(object):
  """ Encapsulates details for a deleted entity version. """
  __SLOTS__ = [u'project_id', u'namespace', u'path', u'original_vs',
               u'deleted_vs']

  def __init__(self, project_id, namespace, path, original_vs, deleted_vs):
    self.project_id = project_id
    self.namespace = namespace
    self.path = path
    self.original_vs = original_vs
    self.deleted_vs = deleted_vs


class DeletedVersionIndex(object):
  """
  A DeletedVersionIndex handles the encoding and decoding details for deleted
  version references.

  The directory path looks like
  (<project-dir>, 'deleted-versions', <namespace>).

  Within this directory, keys are encoded as
  <scatter-byte> + <deleted-vs> + <path-tuple> + <original-vs>.

  The <scatter-byte> is a single byte determined by hashing the entity path.
  Its purpose is to spread writes more evenly across the cluster and minimize
  hotspots. This is especially important for this index because each write is
  given a new, larger <deleted-vs> value than the last.

  The <deleted-vs> is a 10-byte versionstamp that specifies the commit version
  of the transaction that deleted the entity version.

  The <path-tuple> is an encoded tuple containing the entity path.

  The <original-vs> is a 10-byte versionstamp that specifies the commit version
  of the transaction that originally wrote the entity data.

  None of the keys in this index have values.
  """
  DIR_NAME = u'deleted-versions'

  def __init__(self, directory):
    self.directory = directory

  @property
  def project_id(self):
    return self.directory.get_path()[len(DS_ROOT)]

  @property
  def namespace(self):
    return self.directory.get_path()[len(DS_ROOT) + 2]

  @property
  def del_vs_slice(self):
    """ The portion of keys that contain the deleted versionstamp. """
    prefix_size = len(self.directory.rawPrefix) + 1
    return slice(prefix_size, prefix_size + VS_SIZE)

  @property
  def path_slice(self):
    """ The portion of keys that contain the encoded path. """
    return slice(self.del_vs_slice.stop, -1 * VS_SIZE)

  @property
  def original_vs_slice(self):
    """ The portion of keys that contain the original versionstamp. """
    return slice(-1 * VS_SIZE, None)

  def encode_key(self, path, original_vs, deleted_vs=None):
    """ Encodes a key for a deleted version index entry.

    Args:
      path: A tuple or protobuf path object.
      original_vs: A 10-byte string specifying the entity version's original
        commit versionstamp.
      deleted_vs: A 10-byte string specifying the entity version's delete
        versionstamp or None.
    Returns:
      A string containing an FDB key. If deleted_vs was None, the key should
      be used with set_versionstamped_key.
    """
    if not isinstance(path, tuple):
      path = encode_path(path)

    scatter_byte = hash_tuple(path)
    key = b''.join([self.directory.rawPrefix, scatter_byte,
                    deleted_vs or b'\x00' * VS_SIZE,
                    fdb.tuple.pack(path),
                    original_vs])
    if not deleted_vs:
      vs_index = len(self.directory.rawPrefix) + len(scatter_byte)
      key += encode_vs_index(vs_index)

    return key

  def decode(self, kv):
    """ Decodes a KV to a DeletedVersionEntry.

    Args:
      kv: An FDB KeyValue object.
    Returns:
      A DeletedVersionEntry object.
    """
    deleted_vs = kv.key[self.del_vs_slice]
    path = fdb.tuple.unpack(kv.key[self.path_slice])
    original_vs = kv.key[self.original_vs_slice]
    return DeletedVersionEntry(self.project_id, self.namespace, path,
                               original_vs, deleted_vs)

  def get_slice(self, byte_num, safe_vs):
    """
    Gets the range of keys within a scatter byte up to the given deleted
    versionstamp.

    Args:
      byte_num: An integer specifying a scatter byte value.
      safe_vs: A 10-byte value indicating the latest deleted versionstamp that
        should be considered for deletion.
    Returns:
      A slice specifying the start and stop keys.
    """
    scatter_byte = bytes(bytearray([byte_num]))
    prefix = self.directory.rawPrefix + scatter_byte
    return slice(fdb.KeySelector.first_greater_or_equal(prefix + b'\x00'),
                 fdb.KeySelector.first_greater_than(prefix + safe_vs))


class SafeReadDir(object):
  """
  SafeReadDirs keep track of the most recent garbage collection versionstamps.
  These versionstamps are used to invalidate stale transaction IDs.

  When the datastore requests data inside a transaction, the SafeReadDir is
  first checked to make sure that the garbage collector has not cleaned up an
  entity that was mutated after the start of the transaction. Since a mutation
  always means a new version entry, this ensures that valid reads always have
  access to a version entry older than their read versionstamp.

  Put another way, the newest versionstamp values in this directory are going
  to be older than the transaction duration limit. Therefore, datastore
  transactions that see a newer value than their read versionstamp are no
  longer valid.

  The directory path looks like (<project-dir>, 'safe-read', <namespace>).

  Within this directory, keys are simply a single <scatter-byte> derived from
  the entity group that was involved in the garbage collection. The entity
  group is used in order to cover ancestory queries.

  Values are 10-byte versionstamps that indicate the most recently deleted
  entity version that has been garbage collected.
  """
  DIR_NAME = u'safe-read'

  def __init__(self, directory):
    self.directory = directory

  def encode_key(self, path):
    if not isinstance(path, tuple):
      path = encode_path(path)

    entity_group = path[:2]
    return self.directory.rawPrefix + hash_tuple(entity_group)


class GarbageCollector(object):
  _LOCK_KEY = u'gc-lock'

  # The number of extra seconds to wait before checking which versions are safe
  # to delete. A larger value results in fewer GC transactions. It also results
  # in a more relaxed max transaction duration.
  _DEFERRED_DEL_PADDING = 2

  # Give the deferred deletion process a chance to succeed before grooming.
  _SAFETY_INTERVAL = MAX_TX_DURATION * 2

  # The percantage of scattered index space to groom at a time. This fraction's
  # reciprocal should be a factor of 256.
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
    safe_time = monotonic.monotonic() + MAX_TX_DURATION
    for old_entity, old_vs in entities:
      # TODO: Strip raw properties and enforce a max queue size to keep memory
      # usage reasonable.
      self._queue.append((safe_time, old_entity, old_vs, new_vs))

  @gen.coroutine
  def safe_read_vs(self, tr, key):
    project_id = decode_str(key.app())
    safe_read_dir = SafeReadDir.from_cache(project_id, self._directory_cache)
    safe_read_key = safe_read_dir.encode_key(key.path())
    # A concurrent change to the safe read VS does not affect what the current
    # transaction can read, so "snapshot" is used to reduce conflicts.
    vs = yield self._tornado_fdb.get(tr, safe_read_key, snapshot=True)
    if not vs.present():
      return

    raise gen.Return(vs.value)

  def index_deleted_version(self, tr, version_entry):
    index = DeletedVersionIndex.from_cache(
      version_entry.project_id, version_entry.namespace, self._directory_cache)
    key = index.encode_key(version_entry.path, version_entry.commit_vs)
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
    current_time = monotonic.monotonic()
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
      if tr is None:
        tr = self._db.create_transaction()

      self._hard_delete(tr, old_entity, original_vs, deleted_vs)
      if monotonic.monotonic() > tx_deadline:
        yield self._tornado_fdb.commit(tr)
        break

  @gen.coroutine
  def _groom_projects(self):
    while True:
      try:
        yield self._lock.acquire()
        # TODO: Make the list operation async.
        for project_id in self._directory_cache.root.list(self._db):
          yield self._groom_project(project_id)
      except Exception:
        logger.exception(u'Unexpected error while grooming projects')
        yield gen.sleep(10)

  @gen.coroutine
  def _groom_project(self, project_id):
    project_dir = self._directory_cache.get((project_id,))
    # TODO: Make the list operation async.
    for namespace in project_dir.list(self._db):
      for batch_num in sm.range(int(1 / self._BATCH_PERCENT)):
        ranges = sm.range(batch_num * self._BATCH_COUNT,
                          (batch_num + 1) * self._BATCH_COUNT)
        safe_vs = yield self._newest_vs(project_id, namespace, ranges)
        yield gen.sleep(self._SAFETY_INTERVAL)
        if safe_vs is not None:
          yield self._groom_ranges(project_id, namespace, safe_vs, ranges)

  @gen.coroutine
  def _newest_in_range(self, tr, index, byte_num):
    scatter_byte = bytes(bytearray([byte_num]))
    hash_range = index.directory.range((scatter_byte,))
    kvs = yield self._tornado_fdb.get_range(
      tr, hash_range, limit=1, reverse=True, snapshot=True)
    if not kvs:
      return

    raise gen.Return(index.decode(kvs[0]).deleted_vs)

  @gen.coroutine
  def _newest_vs(self, project_id, namespace, ranges):
    yield self._lock.acquire()
    tr = self._db.create_transaction()
    index = DeletedVersionIndex.from_cache(
      project_id, namespace, self._directory_cache)
    deletion_stamps = yield [self._newest_in_range(tr, index, byte_num)
                             for byte_num in ranges]
    newest_vs = max([vs for vs in deletion_stamps if vs] or [None])
    raise gen.Return(newest_vs)

  @gen.coroutine
  def _groom_ranges(self, project_id, namespace, safe_vs, ranges):
    yield self._lock.acquire()
    tr = self._db.create_transaction()
    tx_deadline = monotonic.monotonic() + 2.5
    index = DeletedVersionIndex.from_cache(
      project_id, namespace, self._directory_cache)
    delete_counts = yield [
      self._groom_range(tr, index, byte_num, safe_vs, tx_deadline)
      for byte_num in ranges]
    yield self._tornado_fdb.commit(tr)
    deleted = sum(delete_counts)
    if deleted:
      logger.debug(u'GC deleted {} entities'.format(deleted))

  @gen.coroutine
  def _groom_range(self, tr, index, byte_num, safe_vs, tx_deadline):
    iterator = KVIterator(tr, self._tornado_fdb,
                          index.get_slice(byte_num, safe_vs))
    deleted = 0
    while True:
      kvs, more = yield iterator.next_page()
      for kv in kvs:
        entry = index.decode(kv)
        entity = yield self._data_manager.get_version_from_path(
          tr, entry.project_id, entry.namespace, entry.path, entry.commit_vs)
        self._hard_delete(tr, entity.decoded, entry.original_vs,
                          entry.deleted_vs)
        deleted += 1

      if not more or monotonic.monotonic() > tx_deadline:
        break

    raise gen.Return(deleted)

  def _hard_delete(self, tr, entity, original_vs, deleted_vs):
    project_id = decode_str(entity.key().app())
    namespace = decode_str(entity.key().name_space())

    self._data_manager.hard_delete(tr, entity.key(), original_vs)
    self._index_manager.hard_delete_entries(tr, entity, original_vs)
    index = DeletedVersionIndex.from_cache(
      project_id, namespace, self._directory_cache)
    del tr[index.encode_key(entity.key().path(), original_vs, deleted_vs)]

    # Keep track of safe versionstamps to invalidate stale txids.
    safe_read_dir = SafeReadDir.from_cache(project_id, self._directory_cache)
    safe_read_key = safe_read_dir.encode_key(entity.key().path())
    tr.byte_max(safe_read_key, deleted_vs)
