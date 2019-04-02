import logging
import random
import time

import fdb
from tornado import gen
from tornado.concurrent import Future as TornadoFuture

fdb.api_version(600)
logger = logging.getLogger(__name__)

MAX_FDB_TX_DURATION = 5

_MAX_SEQUENTIAL_BIT = 52
_MAX_SEQUENTIAL_ID = (1 << _MAX_SEQUENTIAL_BIT) - 1
_MAX_SCATTERED_COUNTER = (1 << (_MAX_SEQUENTIAL_BIT - 1)) - 1
_MAX_SCATTERED_ID = _MAX_SEQUENTIAL_ID + 1 + _MAX_SCATTERED_COUNTER
_SCATTER_SHIFT = 64 - _MAX_SEQUENTIAL_BIT + 1

# The Cloud Datastore API uses microseconds as version IDs. When the entity
# doesn't exist, it reports the version as "1".
ABSENT_VERSION = 1

# The max number of bytes for each FDB value.
CHUNK_SIZE = 10000


class EncodedTypes(object):
  ENTITY_V3 = b'\x00'
  KEY_V3 = b'\x01'


def ReverseBitsInt64(v):
  v = ((v >> 1) & 0x5555555555555555) | ((v & 0x5555555555555555) << 1)
  v = ((v >> 2) & 0x3333333333333333) | ((v & 0x3333333333333333) << 2)
  v = ((v >> 4) & 0x0F0F0F0F0F0F0F0F) | ((v & 0x0F0F0F0F0F0F0F0F) << 4)
  v = ((v >> 8) & 0x00FF00FF00FF00FF) | ((v & 0x00FF00FF00FF00FF) << 8)
  v = ((v >> 16) & 0x0000FFFF0000FFFF) | ((v & 0x0000FFFF0000FFFF) << 16)
  v = int((v >> 32) | (v << 32) & 0xFFFFFFFFFFFFFFFF)
  return v


class ScatteredAllocator(object):
  def __init__(self):
    self._counter = random.randint(1, _MAX_SCATTERED_COUNTER)

  def invalidate(self):
    self._counter = random.randint(1, _MAX_SCATTERED_COUNTER)

  def get_id(self):
    id_ = (_MAX_SEQUENTIAL_ID + 1 +
           long(ReverseBitsInt64(self._counter << _SCATTER_SHIFT)))

    self._counter += 1
    if self._counter > _MAX_SCATTERED_COUNTER:
      self._counter = 1

    return id_


class DirectoryCache(object):
  """ Manages a cache of recently opened directories. """

  # The number of items the cache can hold.
  SIZE = 512

  def __init__(self, db, root):
    """ Creates a new DirectoryCache. """
    self.root = root
    self._db = db
    self._directory_list = []
    self._directory_dict = {}

  def get(self, path, base=None, create_if_necessary=True):
    if base is not None:
      root_elements = len(self.root.get_path())
      if base.get_path()[:root_elements] != self.root.get_path():
        raise Exception('Invalid base')

      base_path = base.get_path()[root_elements:]
      path = base_path + path

    try:
      return self._directory_dict[path]
    except KeyError:
      method = self.root.open
      if create_if_necessary:
        method = self.root.create_or_open

      self[path] = method(self._db, path)
      return self[path]

  def __setitem__(self, key, value):
    """ Adds a new directory to the cache.

    Args:
      key: A tuple identifying the directory path.
      value: A directory object.
    """
    if key in self._directory_dict:
      return

    self._directory_dict[key] = value
    self._directory_list.append(key)
    if len(self._directory_list) > self.SIZE:
      old_key = self._directory_list.pop(0)
      del self._directory_dict[old_key]

  def __getitem__(self, key):
    """ Retrieves a directory from the cache.

    Args:
      key: A tuple identifying the directory path.
    """
    return self._directory_dict[key]


class TornadoFDB(object):
  def __init__(self, io_loop):
    self._io_loop = io_loop

  def commit(self, tr):
    tornado_future = TornadoFuture()
    callback = lambda fdb_future: self._handle_fdb_result(
      fdb_future, tornado_future)
    commit_future = tr.commit()
    commit_future.on_ready(callback)
    return tornado_future

  def get(self, tr, key):
    tornado_future = TornadoFuture()
    callback = lambda fdb_future: self._handle_fdb_result(
      fdb_future, tornado_future)
    get_future = tr.get(key)
    get_future.on_ready(callback)
    return tornado_future

  def get_range(self, tr, key_slice, limit=0,
                streaming_mode=fdb.StreamingMode.iterator, iteration=1,
                reverse=False, snapshot=False):
    tx_reader = tr
    if snapshot:
      tx_reader = tr.snapshot

    begin = key_slice.start
    if not isinstance(begin, fdb.KeySelector):
      begin = fdb.KeySelector.first_greater_or_equal(begin)

    end = key_slice.stop
    if not isinstance(end, fdb.KeySelector):
      end = fdb.KeySelector.first_greater_or_equal(end)

    tornado_future = TornadoFuture()
    callback = lambda fdb_future: self._handle_fdb_result(
      fdb_future, tornado_future)

    get_future = tx_reader._get_range(begin, end, limit, streaming_mode,
                                      iteration, reverse)

    get_future.on_ready(callback)
    return tornado_future

  def _handle_fdb_result(self, fdb_future, tornado_future):
    try:
      result = fdb_future.wait()
    except Exception as fdb_error:
      self._io_loop.add_callback(tornado_future.set_exception, fdb_error)
      return

    self._io_loop.add_callback(tornado_future.set_result, result)


class KVIterator(object):
  def __init__(self, tr, tornado_fdb, key_slice, limit=0, reverse=False,
               streaming_mode=fdb.StreamingMode.iterator, snapshot=False):
    self.slice = key_slice
    self.done_with_range = False
    self._tr = tr
    self._tornado_fdb = tornado_fdb

    self._limit = limit
    self._reverse = reverse
    self._mode = streaming_mode
    self._snapshot = snapshot

    self._bsel = key_slice.start
    if not isinstance(self._bsel, fdb.KeySelector):
      self._bsel = fdb.KeySelector.first_greater_or_equal(self._bsel)

    self._esel = key_slice.stop
    if not isinstance(self._esel, fdb.KeySelector):
      self._esel = fdb.KeySelector.first_greater_or_equal(self._esel)

    self._fetched = 0
    self._iteration = 1
    self._index = 0
    self._done = False

  def __repr__(self):
    # TODO: Simplify when KeySelector repr is fixed.
    start = self.slice.start
    if isinstance(start, fdb.KeySelector):
      start = u'KeySelector(%r, %r, %r)' % (start.key, start.or_equal,
                                            start.offset)
    else:
      start = repr(start)

    stop = self.slice.stop
    if isinstance(stop, fdb.KeySelector):
      stop = u'KeySelector(%r, %r, %r)' % (stop.key, stop.or_equal,
                                           stop.offset)
    else:
      stop = repr(stop)

    attrs = [u'start={}'.format(start), u'stop={}'.format(stop)]
    if self._limit > 0:
      attrs.append(u'limit={}'.format(self._limit))

    if self._reverse:
      attrs.append(u'reverse={}'.format(self._reverse))

    return u'KVIterator({})'.format(', '.join(attrs))

  def increase_limit(self, difference=1):
    if not self.done_with_range:
      self._limit += difference
      self._done = False

  @gen.coroutine
  def next_page(self):
    if self._done:
      raise gen.Return(([], False))

    tmp_limit = 0
    if self._limit > 0:
      tmp_limit = self._limit - self._fetched

    logger.debug('start key: %r' % self._bsel.key)
    kvs, count, more = yield self._tornado_fdb.get_range(
      self._tr, slice(self._bsel, self._esel), tmp_limit, self._mode,
      self._iteration, self._reverse, self._snapshot)
    self._fetched += count
    self._iteration += 1

    if kvs:
      if self._reverse:
        self._esel = fdb.KeySelector.first_greater_or_equal(kvs[-1].key)
      else:
        self._bsel = fdb.KeySelector.first_greater_than(kvs[-1].key)

    reached_limit = self._limit > 0 and self._fetched == self._limit
    self._done = not more or reached_limit
    self.done_with_range = not more and not reached_limit

    raise gen.Return((kvs, not self._done))


def next_entity_version(old_version):
  # Since client timestamps are unreliable, ensure the new version is greater
  # than the old one.
  return max(int(time.time() * 1000 * 1000), old_version + 1)


def put_chunks(tr, chunk, subspace, add_vs, chunk_size=CHUNK_SIZE):
  chunk_indexes = [(n, n + chunk_size)
                   for n in xrange(0, len(chunk), chunk_size)]
  for start, end in chunk_indexes:
    value = chunk[start:end]
    if add_vs:
      key = subspace.pack_with_versionstamp((fdb.tuple.Versionstamp(), start))
      tr.set_versionstamped_key(key, value)
    else:
      key = subspace.pack((start,))
      tr[key] = value
