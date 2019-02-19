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

  @gen.coroutine
  def start(self):
    IOLoop.current().spawn_callback(self._run)

  @gen.coroutine
  def acquire(self):
    # Since there is no automatic event timeout, the condition is checked
    # before every acquisition.
    if self._owner != self._client_id or time.time() > self._deadline:
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

    if lease_value is None:
      self._owner = None
    else:
      self._owner, new_op_id = fdb.tuple.unpack(lease_value)
      if new_op_id != self._op_id:
        self._deadline = time.time() + self._LEASE_TIMEOUT
        self._op_id = new_op_id

    if self._owner in (None, self._client_id) or time.time() > self._deadline:
      op_id = uuid.uuid4()
      tr[self.key] = fdb.tuple.pack((self._client_id, op_id))
      yield self._tornado_fdb.commit(tr)
      self._owner = self._client_id
      self._op_id = op_id
      self._deadline = time.time() + self._LEASE_TIMEOUT
      self._event.set()
      logger.info('Acquired lock for {}'.format(self.key))
      yield gen.sleep(self._HEARTBEAT_INTERVAL)
      return

    yield gen.sleep(max(self._deadline - time.time(), 0))


class GarbageCollector(object):
  def __init__(self, db, tornado_fdb, lock, directory_cache):
    self._db = db
    self._tornado_fdb = tornado_fdb
    self._lock = lock
    self._directory_cache = directory_cache

    self._disposable_keys = []

  def start(self):
    IOLoop.current().spawn_callback(self._run_under_lock)

  @gen.coroutine
  def _run_under_lock(self):
    while True:
      try:
        yield self._clean_garbage()
      except Exception:
        logger.exception('Unable to clean garbage')
        gen.sleep(10)

  @gen.coroutine
  def _clean_garbage(self):
    tr = self._db.create_transaction()
    work_cutoff = time.time() + MAX_FDB_TX_DURATION / 2
    versions_deleted = 0
    while self._disposable_keys:
      safe_timestamp, gc_dir, key = self._disposable_keys.pop()
      yield gen.sleep(max(safe_timestamp - time.time(), 0))

      yield self._lock.acquire()
      safe_range = slice(gc_dir.range().start,
                         fdb.KeySelector.first_greater_than(key))
      iterator = RangeIterator(self._tornado_fdb, tr, safe_range)
      while True:
        try:
          kv = yield iterator.next()
        except StopIteration:
          break

        data_path = gc_dir.get_path()[:-1] + ('data',)
        data_dir = self._directory_cache.get(data_path)
        path_with_version = fdb.tuple.unpack(kv.value)
        del tr[data_dir.subspace(path_with_version)]
        del tr[kv.key]
        versions_deleted += 1
        if time.time() > work_cutoff:
          break

      if time.time() > work_cutoff:
        break

    yield self._tornado_fdb.commit(tr)
    logger.info('Cleaned up {} old entity versions'.format(versions_deleted))

    yield self._lock.acquire()

    # Record key ranges that will be disposable after a safe time period.
    tr = self._db.create_transaction()
    self._disposable_keys = []
    ds_dir = self._directory_cache.root
    project_ids = yield self._tornado_fdb.list_subdirectories(ds_dir)
    for project_id in project_ids:
      project_dir = self._directory_cache.get((project_id,))
      namespaces = yield self._tornado_fdb.list_subdirectories(project_dir)
      for namespace in namespaces:
        gc_dir = self._directory_cache.get(
          (project_id, namespace, 'deleted_versions'))
        kvs, count, more_results = yield self._tornado_fdb.get_range(
          tr, gc_dir.range(), limit=1, reverse=True, snapshot=True)
        if not count:
          continue

        safe_timestamp = time.time() + MAX_TX_DURATION
        self._disposable_keys.append((safe_timestamp, gc_dir, kvs[0].key))

    if not self._disposable_keys:
      yield gen.sleep(MAX_TX_DURATION)
