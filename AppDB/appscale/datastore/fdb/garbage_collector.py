"""
We don't want to depend on client timestamps. We could infer the interval that's
passed from the versionstamp, but we don't want to do that either.
We don't need an HA garbage collector. The only time garbage can build up is
if a datastore server isn't able to clean up after itself.

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
      acceptable. Unlike ZooKeeper and etcd, FoundationDB does not have a way
      to specify that a key should be automatically deleted if a client does
      not heartbeat at a regular interval. This implementation requires the
      leader to update the key at regular intervals to indicate that it is
      still alive. All the other lock candidates check at a longer interval to
      see if the leader has stopped updating the key.
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

    yield gen.sleep(max(self._deadline - time.time(), 0))


class GarbageCollector(object):
  def __init__(self, db, tornado_fdb, lock, directory_cache):
    self._db = db
    self._tornado_fdb = tornado_fdb
    self._lock = lock
    self._directory_cache = directory_cache

  def start(self):
    IOLoop.current().spawn_callback(self._run_under_lock)

  @gen.coroutine
  def _run_under_lock(self):
    while True:
      try:
        # yield self._lock.acquire()
        yield self._clean_garbage()
      except Exception:
        logger.exception('Unable to clean garbage')
        yield gen.sleep(10)

  @gen.coroutine
  def _clean_garbage(self):
    # Record key ranges that will be disposable after a safe time period.
    disposable_ranges = []
    ds_dir = self._directory_cache.root
    tr = self._db.create_transaction()
    project_dirs = yield self._tornado_fdb.list_subdirectories(tr, ds_dir)
    logger.debug('project_dirs: {}'.format(project_dirs))
    for project_dir in project_dirs:
      namespace_dirs = yield self._tornado_fdb.list_subdirectories(
        tr, project_dir)
      logger.debug('namespace_dirs: {}'.format(namespace_dirs))
      for namespace_dir in namespace_dirs:
        project_id, namespace = namespace_dir.get_path()[-2:]
        gc_dir = self._directory_cache.get(
          (project_id, namespace, 'deleted_versions'))
        logger.debug('gc_dir: {}'.format(gc_dir))
        kvs, count, more_results = yield self._tornado_fdb.get_range(
          tr, gc_dir.range(), limit=1, reverse=True, snapshot=True)
        if not count:
          continue

        data_dir = self._directory_cache.get((project_id, namespace, 'data'))
        disposable_range = slice(
          gc_dir.range().start, fdb.KeySelector.first_greater_than(kvs[0].key))
        logger.debug('disposable_range: {}'.format(disposable_range))
        disposable_ranges.append((disposable_range, data_dir))

    if not disposable_ranges:
      # yield gen.sleep(MAX_TX_DURATION)
      yield gen.sleep(5)
      return

    # Wait until any existing transactions to expire before removing the
    # deleted versions.
    # yield gen.sleep(MAX_TX_DURATION)
    # if not self._lock.acquired:
    #   return

    versions_deleted = 0
    for disposable_range, data_dir in disposable_ranges:
      work_cutoff = time.time() + MAX_FDB_TX_DURATION / 2
      tr = self._db.create_transaction()
      iterator = RangeIterator(self._tornado_fdb, tr, disposable_range)
      logger.debug('iterator: {}'.format(disposable_range))
      while True:
        try:
          kv = yield iterator.next()
        except StopIteration:
          break

        logger.debug('kv*: {}'.format(kv))
        path_with_version = fdb.tuple.unpack(kv.value)
        del tr[data_dir.subspace(path_with_version)]
        del tr[kv.key]
        versions_deleted += 1
        if time.time() > work_cutoff:
          break

      yield self._tornado_fdb.commit(tr)
      if versions_deleted:
        logger.info('Removed {} old entity versions'.format(versions_deleted))
