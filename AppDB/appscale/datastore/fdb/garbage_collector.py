"""
We don't want to depend on client timestamps. We could infer the interval that's
passed from the versionstamp, but we don't want to do that either.
We don't need an HA garbage collector. The only time garbage can build up is
if a datastore server isn't able to clean up after itself.

TODO: Retry some operations when they fail.
"""

import logging
import time
import uuid

from tornado import gen
from tornado.ioloop import IOLoop
from tornado.locks import Event

from appscale.datastore.fdb.utils import fdb

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
        yield self._lock.acquire()
        yield self._clean_garbage()
      except Exception:
        logger.exception('Unable to clean garbage')
        gen.sleep(10)

  @gen.coroutine
  def _clean_garbage(self):
    tr = self._db.create_transaction()
    # TODO: Get and clean entries

    ds_dir = self._directory_cache.root
    project_ids = yield self._tornado_fdb.list_subdirectories(ds_dir)
    for project_id in project_ids:
      project_dir = self._directory_cache.get((project_id,))
      namespaces = yield self._tornado_fdb.list_subdirectories(project_dir)
      for 
    return
