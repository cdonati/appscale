from tornado.ioloop import PeriodicCallback


class GarbageCollector(object):
  # The number of seconds to wait before trying to claim the lease.
  _LEASE_TIMEOUT = 60

  # The number of seconds to wait before updating the lease.
  _HEARTBEAT_INTERVAL = _LEASE_TIMEOUT / 10

  def __init__(self, db):
    self._db = db

  def start(self):
    while True:
