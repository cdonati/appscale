""" A datastore implementation that uses FoundationDB.

An entity key looks like this:
  [directory-------------------------------------] (compressed)
  (appscale, datastore, <project-id>, <namespace>,
   <element1_type>, <element1_id_or_name>, ..., txid, chunk_index)

The first byte of an entity value indicates the type of object that is stored.
Due to FDB's value size limit, values that exceed the chunk size threshold are
split into multiple key-values.
"""
from __future__ import absolute_import

import logging
import random
import sys

import fdb
from tornado import gen
from tornado.concurrent import Future as TornadoFuture
from tornado.ioloop import IOLoop

from appscale.common.unpackaged import APPSCALE_PYTHON_APPSERVER
from appscale.datastore.dbconstants import BadRequest

sys.path.append(APPSCALE_PYTHON_APPSERVER)
from google.appengine.datastore import entity_pb

fdb.api_version(600)
logger = logging.getLogger(__name__)


_MAX_SEQUENTIAL_BIT = 52
_MAX_SEQUENTIAL_ID = (1 << _MAX_SEQUENTIAL_BIT) - 1
_MAX_SCATTERED_COUNTER = (1 << (_MAX_SEQUENTIAL_BIT - 1)) - 1
_MAX_SCATTERED_ID = _MAX_SEQUENTIAL_ID + 1 + _MAX_SCATTERED_COUNTER
_SCATTER_SHIFT = 64 - _MAX_SEQUENTIAL_BIT + 1


class EntityTypes(object):
  ENTITY_V3 = '0'


class EntitySections(object):
  DATA = '1'
  JOURNAL = '0'


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


# class RangeIterator(object):
#     def __init__(self, tr, begin, end, limit, reverse, streaming_mode):
#       self._tr = tr
#
#       self._bsel = begin
#       self._esel = end
#
#       self._limit = limit
#       self._reverse = reverse
#       self._mode = streaming_mode
#
#       self._future = self._tr._get_range(begin, end, limit, streaming_mode, 1, reverse)
#
#     def to_list(self):
#       if self._mode == StreamingMode.iterator:
#         if self._limit > 0:
#           mode = StreamingMode.exact
#         else:
#           mode = StreamingMode.want_all
#       else:
#         mode = self._mode
#
#       return list(self.__iter__(mode=mode))
#
#     @gen.coroutine
#     def __iter__(self, mode=None):
#       if mode is None:
#         mode = self._mode
#       bsel = self._bsel
#       esel = self._esel
#       limit = self._limit
#
#       iteration = 1  # the first read was fired off when the FDBRange was initialized
#       future = self._future
#
#       done = False
#
#       while not done:
#         if future:
#           (kvs, count, more) = future.wait()
#           index = 0
#           future = None
#
#           if not count:
#             return
#
#         result = kvs[index]
#         index += 1
#
#         if index == count:
#           if not more or limit == count:
#             done = True
#           else:
#             iteration += 1
#             if limit > 0:
#               limit = limit - count
#             if self._reverse:
#               esel = KeySelector.first_greater_or_equal(kvs[-1].key)
#             else:
#               bsel = KeySelector.first_greater_than(kvs[-1].key)
#             future = self._tr._get_range(bsel, esel, limit, mode, iteration, self._reverse)
#
#         yield result


class FDBDatastore(object):
  """ A datastore implementation that uses FoundationDB.
      This is experimental. Don't use it in production. """

  # The max number of bytes for each chunk in an encoded entity.
  _CHUNK_SIZE = 10000

  def __init__(self):
    self._db = None
    self._ds_dir = None
    self._scattered_allocator = ScatteredAllocator()
    self._tornado_fdb = None

  def start(self):
    self._db = fdb.open()
    self._ds_dir = fdb.directory.create_or_open(
      self._db, ('appscale', 'datastore'))
    self._tornado_fdb = TornadoFDB(IOLoop.current())

  @gen.coroutine
  def dynamic_put(self, project_id, put_request, put_response):
    if put_request.has_transaction():
      raise BadRequest('Transactions are not implemented')

    if put_request.auto_id_policy() != put_request.CURRENT:
      raise BadRequest('Sequential allocator is not implemented')

    namespaces = {(entity.key().app(), entity.key().name_space())
                  for entity in put_request.entity_list()}

    # Ensure the client is not performing mutations for a different project.
    invalid_project_id = next((namespace[0] for namespace in namespaces
                               if namespace[0] != project_id), None)
    if invalid_project_id is not None:
      raise BadRequest('Project ID mismatch: '
                       '{} != {}'.format(invalid_project_id, project_id))

    namespace_dirs = {
      ns[1]: self._ds_dir.create_or_open(self._db, (project_id, ns[1]))
      for ns in namespaces}

    futures = []
    for entity in put_request.entity_list():
      namespace_dir = namespace_dirs[entity.key().name_space()]
      futures.append(self._upsert(namespace_dir, entity))

    yield futures

  @gen.coroutine
  def dynamic_get(self, project_id, get_request, get_response):
    if get_request.has_transaction():
      raise BadRequest('Transactions are not implemented')

    namespaces = {(key.app(), key.name_space())
                  for key in get_request.key_list()}

    # Ensure the client is not requesting entities from a different project.
    invalid_project_id = next((namespace[0] for namespace in namespaces
                               if namespace[0] != project_id), None)
    if invalid_project_id is not None:
      raise BadRequest('Project ID mismatch: '
                       '{} != {}'.format(invalid_project_id, project_id))

    namespace_dirs = {
      ns[1]: self._ds_dir.create_or_open(self._db, (project_id, ns[1]))
      for ns in namespaces}

    futures = []
    for key in get_request.key_list():
      namespace_dir = namespace_dirs[key.name_space()]
      futures.append(self._get(namespace_dir, key))

    response = yield futures
    for encoded_entity in response:
      group = get_response.add_entity()
      group.mutable_entity().CopyFrom(entity_pb.EntityProto(encoded_entity))

    raise gen.Return(response)

  @gen.coroutine
  def _get(self, namespace_dir, key):
    path = []
    for element in key.path().element_list():
      if element.has_id():
        path.append([element.type(), element.id()])
      elif element.has_name():
        path.append([element.type(), element.name()])
      else:
        raise BadRequest('All path elements must either have a name or ID')

    if not all(element[1] for element in path[:-1]):
      raise BadRequest('All non-terminal path elements must have an ID or'
                       'name')

    prefix = [item for element in path for item in element]
    tr = self._db.create_transaction()

    key_range = namespace_dir.range(tuple(prefix))
    logger.info('start: {}, {}'.format(key_range.start, fdb.tuple.unpack(key_range.start)))
    logger.info('end: {}, {}'.format(key_range.end, fdb.tuple.unpack(key_range.end)))

    # Select the latest versionstamp for the entity key.
    response = yield self._tornado_fdb.get_range(
      tr, True, key_range.start, key_range.stop, 1, fdb.StreamingMode.want_all,
      1, True)

    kv = response[0][0]
    last_part = kv.value
    key_parts = fdb.tuple.unpack(kv.key)

    # If the entity contains more than one chunk, fetch earlier ones.
    earlier_chunks = []
    if key_parts[-1] > 0:
      begin = namespace_dir.range(tuple(prefix + [key_parts[-2]])).start
      response = yield self._tornado_fdb.get_range(
        tr, True, begin, kv.key, 0, fdb.StreamingMode.want_all, 1, False)
      logger.info('response: {}'.format(response))
      earlier_chunks = [kv.value for kv in response[0]]

    tr.cancel()

    encoded_value = ''.join(earlier_chunks + [last_part])
    entity_type = encoded_value[0]
    if entity_type != ENTITY_V3:
      raise Exception('unknown value')

    raise gen.Return(encoded_value[1:])

  @gen.coroutine
  def _upsert(self, namespace_dir, entity):
    path = []
    for element in entity.key().path().element_list():
      if element.has_id():
        path.append([element.type(), element.id()])
      elif element.has_name():
        path.append([element.type(), element.name()])
      else:
        raise BadRequest('All path elements must either have a name or ID')

    if not all(element[1] for element in path[:-1]):
      raise BadRequest('All non-terminal path elements must have an ID or'
                       'name')

    auto_id = path[-1][1] == 0
    if auto_id:
      path[-1][1] = self._scattered_allocator.get_id()

    prefix = [item for element in path for item in element]
    journal_range = namespace_dir.
    logger.info('prefix: {}'.format(prefix))
    # key_range = namespace_dir.range(
    #   tuple(item for element in path for item in element))
    value = ''.join([ENTITY_V3, entity.Encode()])
    chunk_indexes = [(n, n + self._CHUNK_SIZE)
                     for n in xrange(0, len(value), self._CHUNK_SIZE)]

    tr = self._db.create_transaction()

    # Select the latest version for the entity key.
    response = yield self._tornado_fdb.get_range(
      tr, True, key_range.start, key_range.stop, 1, fdb.StreamingMode.want_all,
      1, True)

    # TODO: Get old value.

    for start, end in chunk_indexes:
      key = namespace_dir.pack_with_versionstamp(
        tuple(prefix + [fdb.tuple.Versionstamp(), start]))
      tr.set_versionstamped_key(key, value[start:end])

    yield self._tornado_fdb.commit(tr)
