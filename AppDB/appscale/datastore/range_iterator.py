import sys
from collections import namedtuple

from appscale.common.unpackaged import APPSCALE_PYTHON_APPSERVER
from appscale.datastore.dbconstants import (
  ASC_PROPERTY_TABLE, BadRequest, KEY_DELIMITER, PROPERTY_SCHEMA,
  TERMINATING_STRING)
from appscale.datastore.utils import decode_path, encode_index_pb

sys.path.append(APPSCALE_PYTHON_APPSERVER)
from google.appengine.datastore.datastore_pb import Query_Filter


IndexEntry = namedtuple('IndexEntry',
                        ['encoded_path', 'entity_reference', 'key', 'path'])


class RangeIterator(object):
  """ Keeps track of a range of index entries.

  This was designed just for merge join queries, and the range can only be
  narrowed.
  """
  CHUNK_SIZE = 1000

  def __init__(self, db, project_id, namespace, kind, prop_name, value,
               ancestor):
    """ Creates a new IndexReader.

    Args:
      db: A database interface object.
      project_id: A string specifying a project ID.
      namespace: A string specifying a namespace.
      kind: A string specifying an entity kind.
      prop_name: A string specifying a property name.
      value: An entity_pb.PropertyValue.
      ancestor: An entity_pb.Reference object.
    """
    self.project_id = project_id
    self.namespace = namespace
    self.kind = kind
    self.prop_name = prop_name

    self._db = db
    self._value = value
    self._ancestor = ancestor

    self._start_key = self._full_key_from_path(ancestor)
    self._end_key = ''.join([self._start_key, TERMINATING_STRING])

    self._cache = []
    self._cache_cursor = None
    self._exhausted = False
    self._inclusive = True

  def __iter__(self):
    return self

  @classmethod
  def from_filter(cls, db, project_id, namespace, kind, pb_filter, ancestor):
    # Make sure this filter can be used for a merge join.
    if pb_filter.op() != Query_Filter.EQUAL:
      raise BadRequest('Invalid filter for merge join '
                       '(op must be equal): {}'.format(pb_filter))

    if pb_filter.property_size() != 1:
      raise BadRequest('Invalid filter for merge join '
                       '(multiple properties): {}'.format(pb_filter))

    property_ = pb_filter.property(0)
    if property_.name() == '__key__':
      raise BadRequest('Invalid property for merge join '
                       '(must not be __key__): {}'.format(property_))

    return cls(db, project_id, namespace, kind, property_.name(),
               property_.value(), ancestor)

  def set_cursor(self, path, inclusive=True):
    key = self._full_key_from_path(path)
    if key < self._start_key:
      raise BadRequest(
        'Start key smaller than previous start key '
        '({} < {})'.format(repr(key), repr(self._start_key)))

    if key > self._end_key:
      raise BadRequest('Start key larger than end key '
                       '({} > {})'.format(repr(key), repr(self._end_key)))

    self._start_key = key
    self._cache_cursor = None
    self._inclusive = inclusive

  def next(self):
    cursor_outside_range = False
    if (self._cache_cursor is not None
        and self._cache_cursor >= len(self._cache)):
      cursor_outside_range = True

    cache_within_range = (self._cache and
                          self._cache[-1].keys()[0] >= self._start_key and
                          not cursor_outside_range)
    if not cache_within_range and self._exhausted:
      raise StopIteration()

    if not cache_within_range:
      self._update_cache()

    if not self._cache:
      raise StopIteration()

    if self._cache_cursor is None:
      self._set_cursor()

    entry = self._cache[self._cache_cursor]
    self._cache_cursor += 1

    entry_key = entry.keys()[0]
    encoded_path = entry_key.rsplit(KEY_DELIMITER)[-1]
    path = decode_path(encoded_path)
    return IndexEntry(encoded_path, entry.values()[0]['reference'], entry_key,
                      path)

  def _update_cache(self):
    self._cache = self._db.range_query(
      ASC_PROPERTY_TABLE, PROPERTY_SCHEMA, self._start_key, self._end_key,
      self.CHUNK_SIZE, start_inclusive=self._inclusive, end_inclusive=True)
    self._inclusive = True
    self._cache_cursor = None

    if len(self._cache) < self.CHUNK_SIZE:
      self._exhausted = True

  def _set_cursor(self):
    """ Bisects the cache to find the first key that is >= the start key. """
    lo = 0
    hi = len(self._cache)
    while lo < hi:
      mid = (lo + hi) // 2
      if self._cache[mid].keys()[0] < self._start_key:
        lo = mid + 1
      else:
        hi = mid

    # If the index is outside the list, it's not valid.
    if lo == len(self._cache):
      raise StopIteration()

    self._cache_cursor = lo

  def _full_key_from_path(self, path):
    """ Creates a full index key from a given path.

    Args:
      path: An entity_pb.Path object.
    Returns:
      A string containing an encoded index key.
    """
    key = KEY_DELIMITER.join(
      [self.project_id, self.namespace, self.kind, self.prop_name,
       str(encode_index_pb(self._value))])

    if path is not None:
      key = KEY_DELIMITER.join([key, str(encode_index_pb(path))])

    return key
