""" A datastore implementation that uses FoundationDB. """
from __future__ import absolute_import

import logging
import random

import fdb
from tornado import gen

from appscale.datastore.dbconstants import BadRequest

fdb.api_version(600)
logger = logging.getLogger(__name__)


_MAX_SEQUENTIAL_BIT = 52
_MAX_SEQUENTIAL_ID = (1 << _MAX_SEQUENTIAL_BIT) - 1
_MAX_SCATTERED_COUNTER = (1 << (_MAX_SEQUENTIAL_BIT - 1)) - 1
_MAX_SCATTERED_ID = _MAX_SEQUENTIAL_ID + 1 + _MAX_SCATTERED_COUNTER
_SCATTER_SHIFT = 64 - _MAX_SEQUENTIAL_BIT + 1


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


class FDBDatastore(object):
  """ A datastore implementation that uses FoundationDB.
      This is experimental. Don't use it in production. """
  def __init__(self):
    self._db = None
    self._ds_dir = None
    self._scattered_allocator = ScatteredAllocator()

  def start(self):
    self._db = fdb.open()
    self._ds_dir = fdb.directory.create_or_open(
      self._db, ('appscale', 'datastore'))

  @gen.coroutine
  def dynamic_put(self, project_id, put_request, put_response):
    if put_request.has_transaction():
      raise BadRequest('Transactions are not implemented')

    if put_request.auto_id_policy() != put_request.CURRENT:
      raise BadRequest('Sequential allocator is not implemented')

    namespaces = {(entity.key().app(), entity.key().name_space())
                  for entity in put_request.entity_list()}

    # Ensure the client is not performing mutations for a different project.
    logger.info('namespaces: {}'.format(namespaces))
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

    logger.info('auto_id: {}'.format(auto_id))
    logger.info('path: {}'.format(path))
    key_range = namespace_dir.range(
      tuple(item for element in path for item in element))
    logger.info('key_range: {}'.format(key_range))
