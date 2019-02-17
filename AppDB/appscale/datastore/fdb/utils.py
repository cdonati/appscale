import random
import time

import fdb
from tornado.concurrent import Future as TornadoFuture

from appscale.datastore.dbconstants import BadRequest, MAX_TX_DURATION

fdb.api_version(600)

_MAX_SEQUENTIAL_BIT = 52
_MAX_SEQUENTIAL_ID = (1 << _MAX_SEQUENTIAL_BIT) - 1
_MAX_SCATTERED_COUNTER = (1 << (_MAX_SEQUENTIAL_BIT - 1)) - 1
_MAX_SCATTERED_ID = _MAX_SEQUENTIAL_ID + 1 + _MAX_SCATTERED_COUNTER
_SCATTER_SHIFT = 64 - _MAX_SEQUENTIAL_BIT + 1


class EntityTypes(object):
  ENTITY_V3 = '0'


class GCActions(object):
  REMOVE_ENTITY_VERSION = 1


def ReverseBitsInt64(v):
  """Reverse the bits of a 64-bit integer.

  Args:
    v: Input integer of type 'int' or 'long'.

  Returns:
    Bit-reversed input as 'int' on 64-bit machines or as 'long' otherwise.
  """
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

  def __init__(self):
    """ Creates a new DirectoryCache. """
    self._directory_list = []
    self._directory_dict = {}
    self._db = None
    self._ds_dir = None

  def start(self, db):
    self._db = db
    self._ds_dir = fdb.directory.create_or_open(db, ('appscale', 'datastore'))

  def get(self, path):
    try:
      return self._directory_dict[path]
    except KeyError:
      self[path] = self._ds_dir.create_or_open(self._db, path)
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

  def get_range(self, tr, key_slice, limit=0,
                streaming_mode=fdb.StreamingMode.iterator, iteration=1,
                reverse=False, snapshot=False):
    tx_reader = tr
    if snapshot:
      tx_reader = tr.snapshot

    begin = fdb.KeySelector.first_greater_or_equal(key_slice.start)
    end = fdb.KeySelector.first_greater_or_equal(key_slice.stop)

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


def flat_path(key):
  path = []
  for element in key.path().element_list():
    path.append(element.type())
    if element.has_id():
      path.append(element.id())
    elif element.has_name():
      path.append(element.name())
    else:
      raise BadRequest('All path elements must either have a name or ID')

  return path


def next_entity_version(old_version):
  # Since client timestamps are unreliable, ensure the new version is greater
  # than the old one.
  return max(int(time.time() * 1000 * 1000), old_version + 1)


def gc_entity_value(namespace, path, version):

  fields = ((GCActions.REMOVE_ENTITY_VERSION, MAX_TX_DURATION) +
            namespace +
            tuple(path) +
            (version,))
  return fdb.tuple.pack(fields)
