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

from appscale.datastore.fdb.utils import fdb

logger = logging.getLogger(__name__)


class GarbageCollector(object):
  # The number of seconds to wait before trying to claim the lease.
  _LEASE_TIMEOUT = 60

  # The number of seconds to wait before updating the lease.
  _HEARTBEAT_INTERVAL = int(_LEASE_TIMEOUT / 10)

  def __init__(self):
    self._db = None
    self._client_id = uuid.uuid4()
    self._ds_dir = None
    self._tornado_fdb = None
    self._lease_key = None
    self._lease_owner = None
    self._lease_versionstamp = None
    self._lease_timeout = None

  def start(self, db, directory_cache, tornado_fdb):
    self._ds_dir = directory_cache.ds_dir
    self._db = db
    self._tornado_fdb = tornado_fdb
    self._lease_key = self._ds_dir.pack(('_gc_lease',))
    IOLoop.current().spawn_callback(self._run)

  @gen.coroutine
  def _run(self):
    while True:
      try:
        yield self._clean_garbage()
      except Exception:
        logger.exception('Unexpected GC error')
        gen.sleep(10)

  @gen.coroutine
  def _lease_and_clean(self):
    if self._lease_owner == self._client_id and time.time() < self._lease_timeout:
    if self._lease_owner != self._client_id:
      tr = self._db.create_transaction()
      value = yield self._tornado_fdb.get(tr, self._lease_key)
      if value is not None:
        self._lease_owner, self._lease_versionstamp = fdb.tuple.unpack(value)
        yield gen.sleep(self._LEASE_TIMEOUT)

      self._db[]

    return
