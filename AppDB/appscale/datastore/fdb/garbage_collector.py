"""
TODO: Retry some operations when they fail.
"""
from __future__ import division

import logging
import time
import uuid

from tornado import gen
from tornado.ioloop import IOLoop
from tornado.locks import Event

from appscale.datastore.dbconstants import MAX_TX_DURATION
from appscale.datastore.fdb.utils import (
  fdb, MAX_FDB_TX_DURATION, RangeIterator)

logger = logging.getLogger(__name__)


class PollingLock(object):
  """ Acquires a lock by writing to a key. This is suitable for a leader
      election in cases where some downtime and initial acquisition delay is
      acceptable.

      Unlike ZooKeeper and etcd, FoundationDB does not have a way
      to specify that a key should be automatically deleted if a client does
      not heartbeat at a regular interval. This implementation requires the
      leader to update the key at regular intervals to indicate that it is
      still alive. All the other lock candidates check at a longer interval to
      see if the leader has stopped updating the key.

      Since client timestamps are unreliable, candidates do not know the
      absolute time the key was updated. Therefore, they each wait for the full
      timeout interval before checking the key again.
  """
  # The number of seconds to wait before trying to claim the lease.
  _LEASE_TIMEOUT = 60

  # The number of seconds to wait before updating the lease.
  _HEARTBEAT_INTERVAL = int(_LEASE_TIMEOUT / 10)

  def __init__(self, db, tornado_fdb, key):
    self.key = key
    self._db = db
    self._tornado_fdb = tornado_fdb

    self._client_id = uuid.uuid4()
    self._owner = None
    self._op_id = None
    self._deadline = None
    self._event = Event()

  @property
  def acquired(self):
    if self._deadline is None:
      return False

    return self._owner == self._client_id and time.time() < self._deadline

  @gen.coroutine
  def start(self):
    IOLoop.current().spawn_callback(self._run)

  @gen.coroutine
  def acquire(self):
    # Since there is no automatic event timeout, the condition is checked
    # before every acquisition.
    if not self.acquired:
      self._event.clear()

    yield self._event.wait()

  @gen.coroutine
  def _run(self):
    while True:
      try:
        yield self._acquire_lease()
      except Exception:
        logger.exception('Unable to acquire lease')
        yield gen.sleep(10)

  @gen.coroutine
  def _acquire_lease(self):
    tr = self._db.create_transaction()
    lease_value = yield self._tornado_fdb.get(tr, self.key)

    if lease_value.present():
      self._owner, new_op_id = fdb.tuple.unpack(lease_value)
      if new_op_id != self._op_id:
        self._deadline = time.time() + self._LEASE_TIMEOUT
        self._op_id = new_op_id
    else:
      self._owner = None

    can_acquire = self._owner is None or time.time() > self._deadline
    if can_acquire or self._owner == self._client_id:
      op_id = uuid.uuid4()
      tr[self.key] = fdb.tuple.pack((self._client_id, op_id))
      yield self._tornado_fdb.commit(tr)
      self._owner = self._client_id
      self._op_id = op_id
      self._deadline = time.time() + self._LEASE_TIMEOUT
      self._event.set()
      if can_acquire:
        logger.info('Acquired lock for {}'.format(repr(self.key)))

      yield gen.sleep(self._HEARTBEAT_INTERVAL)
      return

    # Since another candidate holds the lock, wait until it might expire.
    yield gen.sleep(max(self._deadline - time.time(), 0))


class GarbageCollector(object):
  def __init__(self, db, tornado_fdb, lock, directory_cache):
    self._db = db
    self._tornado_fdb = tornado_fdb
    self._lock = lock
    self._directory_cache = directory_cache

  def start(self):
    IOLoop.current().spawn_callback(self._run_under_lock)

  def index_deleted_version(self, tr, namespace, path, version, op_id=0):
    # op_id allows multiple versions to be deleted in a single transaction.
    gc_dir = self._directory_cache.get(namespace + ('deleted_versions',))
    gc_key = gc_dir.pack_with_versionstamp((fdb.tuple.Versionstamp(), op_id))
    gc_val = fdb.tuple.pack(path + (version,))
    tr.set_versionstamped_key(gc_key, gc_val)

  @gen.coroutine
  def clear_version(self, namespace, path, version, gc_versionstamp, op_id=0,
                    tr=None):
    create_transaction = tr is None
    if create_transaction:
      tr = self._db.create_transaction()

    data_dir = self._directory_cache.get(namespace + ('data',))
    gc_dir = self._directory_cache.get(namespace + ('deleted_verisons',))
    gc_key = gc_dir.pack((gc_versionstamp, op_id))

    del tr[data_dir.subspace(path + (version,)).range()]
    del tr[gc_key]

    if create_transaction:
      yield self._tornado_fdb.commit()

  @gen.coroutine
  def _run_under_lock(self):
    while True:
      try:
        yield self._lock.acquire()
        yield self._clean_garbage()
      except Exception:
        logger.exception('Unable to clean garbage')
        yield gen.sleep(10)

  @gen.coroutine
  def _get_disposable_ranges(self):
    """ Fetches key ranges that will be disposable after a safe time period.

    Returns:
      A list of (namespace, range) tuples that can be cleared later.
    """
    disposable_ranges = []
    ds_dir = self._directory_cache.root
    tr = self._db.create_transaction()
    project_dirs = yield self._tornado_fdb.list_subdirectories(tr, ds_dir)
    for project_dir in project_dirs:
      namespace_dirs = yield self._tornado_fdb.list_subdirectories(
        tr, project_dir)
      for namespace_dir in namespace_dirs:
        namespace = namespace_dir.get_path()[-2:]
        gc_dir = self._directory_cache.get(namespace + ('deleted_versions',))
        kvs, count, more_results = yield self._tornado_fdb.get_range(
          tr, gc_dir.range(), limit=1, reverse=True, snapshot=True)
        if not count:
          continue

        disposable_range = slice(
          gc_dir.range().start, fdb.KeySelector.first_greater_than(kvs[0].key))
        disposable_ranges.append((namespace, disposable_range))

    raise gen.Return(disposable_ranges)

  @gen.coroutine
  def _clear_ranges(self, disposable_ranges):
    versions_deleted = 0
    for namespace, disposable_range in disposable_ranges:
      gc_dir = self._directory_cache.get(namespace + ('deleted_versions',))
      work_cutoff = time.time() + MAX_FDB_TX_DURATION / 2
      tr = self._db.create_transaction()
      iterator = RangeIterator(self._tornado_fdb, tr, disposable_range)
      while True:
        kv = yield iterator.next()
        if kv is None:
          break

        path_with_version = fdb.tuple.unpack(kv.value)
        path = path_with_version[:-1]
        version = path_with_version[-1]
        gc_versionstamp, op_id = gc_dir.unpack(kv.key)

        yield self.clear_version(namespace, path, version, gc_versionstamp,
                                 op_id, tr)
        versions_deleted += 1
        if time.time() > work_cutoff:
          break

      yield self._tornado_fdb.commit(tr)
      if versions_deleted:
        logger.info('Removed {} old entity versions'.format(versions_deleted))

  @gen.coroutine
  def _clean_garbage(self):
    disposable_ranges = yield self._get_disposable_ranges()
    if not disposable_ranges:
      yield gen.sleep(MAX_TX_DURATION)
      return

    # Wait until any existing transactions to expire before removing the
    # deleted versions.
    yield gen.sleep(MAX_TX_DURATION)
    if not self._lock.acquired:
      return

    yield self._clear_ranges(disposable_ranges)
