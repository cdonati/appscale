import logging
import monotonic
import uuid

from tornado import gen
from tornado.ioloop import IOLoop
from tornado.locks import Event

from appscale.datastore.fdb.utils import fdb

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

    return (self._owner == self._client_id and
            monotonic.monotonic() < self._deadline)

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
        self._deadline = monotonic.monotonic() + self._LEASE_TIMEOUT
        self._op_id = new_op_id
    else:
      self._owner = None

    can_acquire = self._owner is None or monotonic.monotonic() > self._deadline
    if can_acquire or self._owner == self._client_id:
      op_id = uuid.uuid4()
      tr[self.key] = fdb.tuple.pack((self._client_id, op_id))
      yield self._tornado_fdb.commit(tr)
      self._owner = self._client_id
      self._op_id = op_id
      self._deadline = monotonic.monotonic() + self._LEASE_TIMEOUT
      self._event.set()
      if can_acquire:
        logger.info('Acquired lock for {}'.format(repr(self.key)))

      yield gen.sleep(self._HEARTBEAT_INTERVAL)
      return

    # Since another candidate holds the lock, wait until it might expire.
    yield gen.sleep(max(self._deadline - monotonic.monotonic(), 0))
