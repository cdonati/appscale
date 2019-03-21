import logging
import random
import sys
import time
import uuid

import fdb
from fdb.directory_impl import DirectorySubspace
from tornado import gen
from tornado.concurrent import Future as TornadoFuture

from appscale.common.unpackaged import APPSCALE_PYTHON_APPSERVER
from appscale.datastore.dbconstants import BadRequest

sys.path.append(APPSCALE_PYTHON_APPSERVER)
from google.appengine.datastore import datastore_pb, entity_pb

fdb.api_version(600)
logger = logging.getLogger(__name__)

# The max number of bytes for each FDB value.
CHUNK_SIZE = 10000

MAX_FDB_TX_DURATION = 5

_MAX_SEQUENTIAL_BIT = 52
_MAX_SEQUENTIAL_ID = (1 << _MAX_SEQUENTIAL_BIT) - 1
_MAX_SCATTERED_COUNTER = (1 << (_MAX_SEQUENTIAL_BIT - 1)) - 1
_MAX_SCATTERED_ID = _MAX_SEQUENTIAL_ID + 1 + _MAX_SCATTERED_COUNTER
_SCATTER_SHIFT = 64 - _MAX_SEQUENTIAL_BIT + 1


class EncodedTypes(object):
  ENTITY_V3 = '0'
  KEY_V3 = '1'


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
  def __init__(self, tr, tornado_fdb, key_slice, limit=0, reverse=False,
               streaming_mode=fdb.StreamingMode.iterator, snapshot=False):
    self._tr = tr
    self._tornado_fdb = tornado_fdb

    self._limit = limit
    self._reverse = reverse
    self._mode = streaming_mode
    self._snapshot = snapshot

    self._bsel = key_slice.start
    self._esel = key_slice.stop
    self._fetched = 0
    self._iteration = 1
    self._cache = []
    self._index = 0
    self._done = False

  @property
  def cache_exhausted(self):
    return self._index == len(self._cache)

  @gen.coroutine
  def next_page(self):
    if self._done:
      raise gen.Return(([], not self._done))

    tmp_limit = 0
    if self._limit > 0:
      tmp_limit = self._limit - self._fetched

    kvs, count, more = yield self._tornado_fdb.get_range(
      self._tr, slice(self._bsel, self._esel), tmp_limit, self._mode,
      self._iteration, self._reverse, self._snapshot)
    self._fetched += count

    if more or self._fetched < self._limit:
      self._iteration += 1
      if self._reverse:
        self._esel = fdb.KeySelector.first_greater_or_equal(kvs[-1].key)
      else:
        self._bsel = fdb.KeySelector.first_greater_than(kvs[-1].key)
    else:
      self._done = True

    raise gen.Return((kvs, not self._done))

  @gen.coroutine
  def next(self):
    if self._done and self.cache_exhausted:
      return

    if self.cache_exhausted:
      self._cache = yield self.next_page()[0]
      self._index = 0

    if not self._cache:
      return

    result = self._cache[self._index]
    self._index += 1
    raise gen.Return(result)


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
  if isinstance(key, entity_pb.PropertyValue_ReferenceValue):
    element_list = key.pathelement_list()
  else:
    element_list = key.path().element_list()

  for element in element_list:
    path.append(element.type())
    if element.has_id():
      path.append(element.id())
    elif element.has_name():
      path.append(element.name())
    else:
      raise BadRequest('All path elements must either have a name or ID')

  return tuple(path)


def decode_path(flattened_path, reference_value=False):
  if len(flattened_path) % 2 != 0:
    raise BadRequest('Invalid path')

  if reference_value:
    path = entity_pb.PropertyValue_ReferenceValue()
  else:
    path = entity_pb.Path()

  index = 0
  while index < len(flattened_path):
    element = path.add_element()
    element.set_type(flattened_path[index])
    id_or_name = flattened_path[index + 1]
    if isinstance(id_or_name, int):
      element.set_id(id_or_name)
    else:
      element.set_name(id_or_name)

    index += 2

  return path


def next_entity_version(old_version):
  # Since client timestamps are unreliable, ensure the new version is greater
  # than the old one.
  return max(int(time.time() * 1000 * 1000), old_version + 1)


def new_txid():
  return uuid.uuid4().int & (1 << 64) - 1


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


def log_request(tr, tx_dir, request):
  txid = request.transaction().handle()
  if isinstance(request, datastore_pb.PutRequest):
    value = fdb.tuple.pack(
      (EncodedTypes.ENTITY_V3,) +
      tuple(entity.Encode() for entity in request.entity_list()))
    subspace = tx_dir.subspace((txid, 'puts'))
  elif isinstance(request, datastore_pb.GetRequest):
    value = fdb.tuple.pack(
      (EncodedTypes.KEY_V3,) +
      tuple(key.Encode() for key in request.key_list()))
    subspace = tx_dir.subspace((txid, 'lookups'))
  elif isinstance(request, datastore_pb.DeleteRequest):
    value = fdb.tuple.pack(
      (EncodedTypes.KEY_V3,) +
      tuple(key.Encode() for key in request.key_list()))
    subspace = tx_dir.subspace((txid, 'deletes'))
  else:
    raise BadRequest('Unexpected RPC type')

  put_chunks(tr, value, subspace, add_vs=True)


def decode_chunks(chunks, rpc_type):
  if rpc_type == 'puts':
    expected_encoding = EncodedTypes.ENTITY_V3
    pb_class = entity_pb.EntityProto
  elif rpc_type in ('lookups', 'deletes'):
    expected_encoding = EncodedTypes.KEY_V3
    pb_class = entity_pb.Reference
  else:
    raise BadRequest('Unexpected RPC type')

  elements = fdb.tuple.unpack(''.join(chunks))
  if elements[0] != expected_encoding:
    raise BadRequest('Unexpected encoding')

  return [pb_class(encoded_value) for encoded_value in elements[1:]]
