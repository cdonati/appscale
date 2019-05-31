"""
This module keeps track of outdated transactions and entity versions and
deletes them when sufficient time has passed. The GarbageCollector is the main
interface that clients can use to mark resources as old and determine if a
resource is still available.
"""
from __future__ import division
import logging
import monotonic
from collections import deque

import six.moves as sm
from tornado import gen
from tornado.ioloop import IOLoop

from appscale.datastore.dbconstants import MAX_TX_DURATION
from appscale.datastore.fdb.cache import NSCache
from appscale.datastore.fdb.codecs import (
  decode_str, encode_path, encode_read_vs, encode_vs_index)
from appscale.datastore.fdb.polling_lock import PollingLock
from appscale.datastore.fdb.utils import (
  DS_ROOT, fdb, hash_tuple, KVIterator, MAX_FDB_TX_DURATION, VS_SIZE)

logger = logging.getLogger(__name__)


class NoProjects(Exception):
  """ Indicates that there are no existing projects. """
  pass


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
  to be older than the datastore transaction duration limit. Therefore,
  datastore transactions that see a newer value than their read versionstamp
  are no longer valid.

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
    """ Encodes a key for a safe read versionstamp entry.

    Args:
      path: A tuple or protobuf path object.
    Returns:
      A string containing an FDB key.
    """
    if not isinstance(path, tuple):
      path = encode_path(path)

    entity_group = path[:2]
    return self.directory.rawPrefix + hash_tuple(entity_group)


class GarbageCollector(object):
  # The FDB key used to acquire a GC lock.
  _LOCK_KEY = u'gc-lock'

  # The number of extra seconds to wait before checking which versions are safe
  # to delete. A larger value results in fewer GC transactions. It also results
  # in a more relaxed max transaction duration.
  _DEFERRED_DEL_PADDING = 30

  # Give the deferred deletion process a chance to succeed before grooming.
  _SAFETY_INTERVAL = MAX_TX_DURATION * 2

  # The percantage of scattered index space to groom at a time. This fraction's
  # reciprocal should be a factor of 256.
  _BATCH_PERCENT = .125

  # The number of ranges to groom within a single transaction.
  _BATCH_SIZE = int(_BATCH_PERCENT * 256)

  # The total number of batches in a directory.
  _TOTAL_BATCHES = int(1 / _BATCH_PERCENT)

  def __init__(self, db, tornado_fdb, data_manager, index_manager,
               project_cache):
    self._db = db
    self._queue = deque()
    self._tornado_fdb = tornado_fdb
    self._data_manager = data_manager
    self._index_manager = index_manager
    self._project_cache = project_cache
    lock_key = self._project_cache.root.pack((self._LOCK_KEY,))
    self._lock = PollingLock(self._db, self._tornado_fdb, lock_key)
    self._del_version_index_cache = NSCache(
      self._tornado_fdb, self._project_cache, DeletedVersionIndex)
    self._safe_read_dir_cache = NSCache(
      self._tornado_fdb, self._project_cache, SafeReadDir)

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
    safe_read_dir = yield self._safe_read_dir_cache.get_from_key(tr, key)
    safe_read_key = safe_read_dir.encode_key(key.path())
    # A concurrent change to the safe read VS does not affect what the current
    # transaction can read, so "snapshot" is used to reduce conflicts.
    vs = yield self._tornado_fdb.get(tr, safe_read_key, snapshot=True)
    if not vs.present():
      return

    raise gen.Return(vs.value)

  @gen.coroutine
  def index_deleted_version(self, tr, version_entry):
    index = yield self._del_version_index_cache.get(
      tr, version_entry.project_id, version_entry.namespace)
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
    tx_deadline = current_time + MAX_FDB_TX_DURATION - 1
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

      yield self._hard_delete(tr, old_entity, original_vs, deleted_vs)
      if monotonic.monotonic() > tx_deadline:
        yield self._tornado_fdb.commit(tr)
        break

  @gen.coroutine
  def _groom_projects(self):
    cursor = (u'', u'', 0)  # Last (project_id, namespace, batch) groomed.
    while True:
      try:
        yield self._lock.acquire()
        tr = self._db.create_transaction()
        safe_version = yield self._tornado_fdb.get_read_version(tr)
        safe_vs = encode_read_vs(safe_version)
        yield gen.sleep(self._SAFETY_INTERVAL)

        yield self._lock.acquire()
        tr = self._db.create_transaction()
        try:
          project_id, namespace, batch = yield self._next_batch(tr, *cursor)
        except NoProjects as error:
          logger.info(str(error))
          yield gen.sleep(self._SAFETY_INTERVAL)
          continue

        yield self._groom_deleted_versions(
          tr, project_id, namespace, batch, safe_vs)
        for project_dir in project_dirs:
          del_version_dirs = yield self._del_version_index_cache.list(
            tr, project_dir)
          for del_version_dir in del_version_dirs:
            yield self._gro
          yield self._groom_project(project_id)
      except Exception:
        logger.exception(u'Unexpected error while grooming projects')
        yield gen.sleep(10)

  @gen.coroutine
  def _next_batch(self, tr, project_id, namespace, batch_num):
    project_dirs = yield self._project_cache.list(tr)
    project_ids = [p_dir.get_path()[-1] for p_dir in project_dirs]
    if not project_ids:
      raise NoProjects(u'There are no projects to groom')

    previous_dir_found = True
    if project_id not in project_ids:
      project_id = next((project for project in project_ids
                         if project > project_id), project_ids[0])
      namespace = u''
      batch_num = 0
      previous_dir_found = False

    project_dir = next(p_dir for p_dir in project_dirs
                       if p_dir.get_path()[-1] == project_id)
    namespace_dirs = yield self._del_version_index_cache.list(tr, project_dir)
    namespaces = [p_dir.namespace for p_dir in namespace_dirs] or [u'']
    if namespace not in namespaces:
      namespace = next((ns for ns in namespaces if ns > namespace),
                       namespaces[0])
      batch_num = 0
      previous_dir_found = False

    if previous_dir_found:
      batch_num += 1

    if batch_num >= self._TOTAL_BATCHES:
      namespace = next((ns for ns in namespaces if ns > namespace), None)
      if namespace is None:
        project_id = next((project for project in project_ids
                           if project > project_id), project_ids[0])
        project_dir = next(p_dir for p_dir in project_dirs
                           if p_dir.get_path()[-1] == project_id)
        namespace_dirs = yield self._del_version_index_cache.list(
          tr, project_dir)
        namespace = next((p_dir.namespace for p_dir in namespace_dirs), u'')

      batch_num = 0

    raise gen.Return((project_id, namespace, batch_num))

  @gen.coroutine
  def _groom_deleted_versions(self, tr, project_id, namespace, batch_num,
                              safe_vs):
    del_version_dir = yield self._del_version_index_cache.get(
      tr, project_id, namespace)
    ranges = sm.range(batch_num * self._BATCH_SIZE,
                      (batch_num + 1) * self._BATCH_SIZE)
    tx_deadline = monotonic.monotonic() + MAX_FDB_TX_DURATION - 1
    delete_counts = yield [
      self._groom_range(tr, del_version_dir, byte_num, safe_vs, tx_deadline)
      for byte_num in ranges]
    yield self._tornado_fdb.commit(tr)
    deleted = sum(delete_counts)
    if deleted:
      logger.debug(u'GC deleted {} entities'.format(deleted))

  def _groom_expired_transactions(self, tr, project_id, batch_num, safe_vs):


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

  @gen.coroutine
  def _hard_delete(self, tr, entity, original_vs, deleted_vs):
    project_id = decode_str(entity.key().app())
    namespace = decode_str(entity.key().name_space())

    yield self._data_manager.hard_delete(tr, entity.key(), original_vs)
    self._index_manager.hard_delete_entries(tr, entity, original_vs)
    index = yield self._del_version_index_cache.get(tr, project_id, namespace)
    del tr[index.encode_key(entity.key().path(), original_vs, deleted_vs)]

    # Keep track of safe versionstamps to invalidate stale txids.
    safe_read_dir = yield self._safe_read_dir_cache.get(
      tr, project_id, namespace)
    safe_read_key = safe_read_dir.encode_key(entity.key().path())
    tr.byte_max(safe_read_key, deleted_vs)
