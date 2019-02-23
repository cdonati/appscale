import logging
import random
import time

import fdb
from fdb.directory_impl import DirectorySubspace
from tornado import gen
from tornado.concurrent import Future as TornadoFuture

from appscale.datastore.dbconstants import BadRequest

fdb.api_version(600)
logger = logging.getLogger(__name__)

MAX_FDB_TX_DURATION = 5

_MAX_SEQUENTIAL_BIT = 52
_MAX_SEQUENTIAL_ID = (1 << _MAX_SEQUENTIAL_BIT) - 1
_MAX_SCATTERED_COUNTER = (1 << (_MAX_SEQUENTIAL_BIT - 1)) - 1
_MAX_SCATTERED_ID = _MAX_SEQUENTIAL_ID + 1 + _MAX_SCATTERED_COUNTER
_SCATTER_SHIFT = 64 - _MAX_SEQUENTIAL_BIT + 1


class EntityTypes(object):
  ENTITY_V3 = '0'


class Directories(object):
  DATA = ('data',)
  DELETED = ('deleted_versions',)


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

  @gen.coroutine
  def list_subdirectories(self, tr, directory):
    subdirectories = []
    more_results = True
    iteration = 1
    while more_results:
      kvs, count, more_results = yield self.get_range(
        tr, subdirs_subspace(directory).range(), iteration=iteration)
      subdirectories.extend([kv_to_dir(directory, kv) for kv in kvs])
      iteration += 1

    raise gen.Return(subdirectories)

  def _handle_fdb_result(self, fdb_future, tornado_future):
    try:
      result = fdb_future.wait()
    except Exception as fdb_error:
      self._io_loop.add_callback(tornado_future.set_exception, fdb_error)
      return

    self._io_loop.add_callback(tornado_future.set_result, result)


class RangeIterator(object):
  def __init__(self, tornado_fdb, tr, key_slice,
               streaming_mode=fdb.StreamingMode.iterator, reverse=False,
               snapshot=False):
    self._tornado_fdb = tornado_fdb
    self._tr = tr
    self._key_slice = key_slice
    self._streaming_mode = streaming_mode
    self._reverse = reverse
    self._snapshot = snapshot

    self._iteration = 1
    self._cache = []
    self._exhausted = False

  @gen.coroutine
  def next(self):
    if self._exhausted and not self._cache:
      return

    if not self._cache:
      kvs, count, more_results = yield self._tornado_fdb.get_range(
        self._tr, self._key_slice, 0, self._streaming_mode, self._iteration,
        self._reverse, self._snapshot)
      self._iteration += 1
      if not more_results:
        self._exhausted = True

      if not count:
        return

      self._cache.extend(kvs)

    raise gen.Return(self._cache.pop(0))


def subdirs_subspace(directory):
  """ Returns the subspace that the directory layer uses to keep track of
      child directories.

  Args:
    directory: The parent DirectorySubspace object.

  Returns:
    A Subspace.
  """
  dir_layer = directory._directory_layer
  parent_subspace = dir_layer._node_with_prefix(directory.rawPrefix)
  return parent_subspace.subspace((dir_layer.SUBDIRS,))


def kv_to_dir(parent, kv):
  name = subdirs_subspace(parent).unpack(kv.key)[0]
  path = parent.get_path() + (name,)
  return DirectorySubspace(path, kv.value)


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

  return tuple(path)


def next_entity_version(old_version):
  # Since client timestamps are unreliable, ensure the new version is greater
  # than the old one.
  return max(int(time.time() * 1000 * 1000), old_version + 1)
