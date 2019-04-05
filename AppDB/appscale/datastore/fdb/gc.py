import logging
import time
from collections import deque

from tornado import gen
from tornado.ioloop import IOLoop

logger = logging.getLogger(__name__)


class GarbageCollector(object):
  def __init__(self, db, tornado_fdb, data_manager, index_manager):
    self._db = db
    self._queue = deque()
    self._tornado_fdb = tornado_fdb
    self._data_manager = data_manager
    self._index_manager = index_manager

  def start(self):
    IOLoop.current().spawn_callback(self._run_forever)

  def clear_later(self, entities):
    safe_time = time.time() + 60
    for old_entity, old_vs in entities:
      self._queue.append((safe_time, old_entity, old_vs))

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
    logger.debug('processing queue')
    current_time = time.time()
    tx_deadline = current_time + 2.5
    tr = None
    while True:
      if not self._queue:
        if tr is not None:
          yield self._tornado_fdb.commit(tr)

        yield gen.sleep(61)
        break

      safe_time, old_entity, old_vs = self._queue.popleft()
      if safe_time > current_time:
        self._queue.appendleft((safe_time, old_entity, old_vs))
        if tr is not None:
          yield self._tornado_fdb.commit(tr)

        yield gen.sleep(safe_time - current_time + 1)
        break

      if tr is None:
        tr = self._db.create_transaction()

      self._data_manager.hard_delete(tr, old_entity.key(), old_vs)
      self._index_manager.hard_delete_entries(tr, old_entity, old_vs)

      if time.time() > tx_deadline:
        yield self._tornado_fdb.commit(tr)
        break
