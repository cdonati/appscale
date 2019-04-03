"""
indexes: This contains a directory for each index that the datastore needs in
order to satisfy basic queries along with indexes that the project has defined
for composite queries. Here is an example template:

  ([index dir^4], <type>, <value>, [path], <commit versionstamp>) -> ''

^4: The index's directory path. For example,
    (appscale, datastore, <project>, indexes, <namespace>, single-property,
     <kind>, <property name>)
"""
from __future__ import division

import itertools
import logging
import sys
import time

import six
from tornado import gen

from appscale.common.unpackaged import APPSCALE_PYTHON_APPSERVER
from appscale.datastore.fdb.codecs import (
  decode_element, decode_path, decode_str, decode_value, encode_ancestor_range,
  encode_path, encode_value)
from appscale.datastore.fdb.utils import fdb, MAX_FDB_TX_DURATION, KVIterator
from appscale.datastore.dbconstants import BadRequest, InternalError
from appscale.datastore.index_manager import IndexInaccessible
from appscale.datastore.utils import _FindIndexToUse

sys.path.append(APPSCALE_PYTHON_APPSERVER)
from google.appengine.datastore import appscale_stub_util
from google.appengine.datastore import datastore_pb, entity_pb
from google.appengine.datastore.datastore_pb import Query_Filter, Query_Order

logger = logging.getLogger(__name__)

INDEX_DIR = u'indexes'

KEY_PROP = u'__key__'

START_FILTERS = (Query_Filter.GREATER_THAN_OR_EQUAL, Query_Filter.GREATER_THAN)
STOP_FILTERS = (Query_Filter.LESS_THAN_OR_EQUAL, Query_Filter.LESS_THAN)


class FilterProperty(object):
  __slots__ = [u'name', u'filters']

  def __init__(self, prop_name, filters):
    self.name = prop_name
    self.filters = filters

  @property
  def equality(self):
    return all(op == Query_Filter.EQUAL for op, _ in self.filters)

  def __repr__(self):
    return u'FilterProperty(%r, %r)' % (self.name, self.filters)


def group_filters(query):
  filter_props = []
  for query_filter in query.filter_list():
    if query_filter.property_size() != 1:
      raise BadRequest(u'Each filter must have exactly one property')

    prop = query_filter.property(0)
    prop_name = decode_str(prop.name())
    filter_info = (query_filter.op(), prop.value())
    if filter_props and filter_props[-1].name == prop_name:
      filter_props[-1].filters.append(filter_info)
    else:
      filter_props.append(FilterProperty(prop_name, [filter_info]))

  # Since the filter list can come in any order, put inequality filters last.
  inequality_index = None
  for index, filter_prop in enumerate(filter_props):
    if not filter_prop.equality:
      inequality_index = index
      break

  if inequality_index is not None:
    inequality_prop = filter_props.pop(inequality_index)
    filter_props.append(inequality_prop)

  # Put key filters last.
  key_index = None
  for index, filter_prop in enumerate(filter_props):
    if filter_prop.name == KEY_PROP:
      key_index = index
      break

  if key_index is not None:
    key_prop = filter_props.pop(key_index)
    filter_props.append(key_prop)

  for filter_prop in filter_props[:-1]:
    if filter_prop.name == KEY_PROP:
      raise BadRequest(
        u'Only the last filter property can be on {}'.format(KEY_PROP))

    if not filter_prop.equality:
      raise BadRequest(u'All but the last property must be equality filters')

  return tuple(filter_props)


def get_order_info(query):
  filter_props = group_filters(query)

  # Orders on equality filters can be ignored.
  equality_props = [prop.name for prop in filter_props if prop.equality]
  relevant_orders = [order for order in query.order_list()
                     if order.property() not in equality_props]

  order_info = []
  for filter_prop in filter_props:
    if filter_prop.equality:
      continue

    direction = next(
      (order.direction() for order in relevant_orders
       if order.property() == filter_prop.name), Query_Order.ASCENDING)
    order_info.append((filter_prop.name, direction))

  filter_prop_names = [prop.name for prop in filter_props]
  order_info.extend(
    [(order.property(), order.direction()) for order in relevant_orders
     if order.property() not in filter_prop_names])

  return tuple(order_info)


def get_scan_direction(query, index):
  order_info = get_order_info(query)
  if not order_info:
    return Query_Order.ASCENDING

  first_property, first_direction = order_info[0]
  if first_property == KEY_PROP or isinstance(index, SinglePropIndex):
    return first_direction

  index_direction = next(direction for prop_name, direction in index.order_info
                         if prop_name == first_property)
  if index_direction == first_direction:
    return Query_Order.ASCENDING
  else:
    return Query_Order.DESCENDING


def get_fdb_key_selector(op, encoded_value):
  """ Like Python's slice notation, FDB range queries include the start and
      exclude the stop. Therefore, the stop selector must point to the first
      key that will be excluded from the results. """
  if op == Query_Filter.GREATER_THAN_OR_EQUAL:
    return fdb.KeySelector.first_greater_or_equal(encoded_value)
  elif op == Query_Filter.GREATER_THAN:
    return fdb.KeySelector.first_greater_than(encoded_value + b'\xff')
  elif op == Query_Filter.LESS_THAN_OR_EQUAL:
    return fdb.KeySelector.first_greater_or_equal(encoded_value + b'\xff')
  elif op == Query_Filter.LESS_THAN:
    return fdb.KeySelector.first_greater_than(encoded_value)
  else:
    raise BadRequest(u'Unsupported filter operator')


class IndexEntry(object):
  __SLOTS__ = [u'project_id', u'namespace', u'path', u'commit_vs',
               u'deleted_vs']

  def __init__(self, project_id, namespace, path, commit_vs, deleted_vs):
    self.project_id = project_id
    self.namespace = namespace
    self.path = path
    self.commit_vs = commit_vs
    if deleted_vs is None:
      deleted_vs = fdb.tuple.Versionstamp()

    self.deleted_vs = deleted_vs

  @property
  def key(self):
    key = entity_pb.Reference()
    key.set_app(self.project_id)
    key.set_name_space(self.namespace)
    path = key.mutable_path()
    path.MergeFrom(decode_path(self.path))
    return key

  @property
  def group(self):
    group = entity_pb.Path()
    group.add_element().MergeFrom(decode_element(self.path[:2]))
    return group

  def __repr__(self):
    return u'IndexEntry(%r, %r, %r, %r, %r)' % (
      self.project_id, self.namespace, self.path, self.commit_vs,
      self.deleted_vs)

  def key_result(self):
    entity = entity_pb.EntityProto()
    entity.mutable_key().MergeFrom(self.key)
    entity.mutable_entity_group()
    return entity

  def cursor_result(self):
    compiled_cursor = datastore_pb.CompiledCursor()
    position = compiled_cursor.add_position()
    position.mutable_key().MergeFrom(self.key)
    position.set_start_inclusive(False)
    return compiled_cursor


class PropertyEntry(IndexEntry):
  __SLOTS__ = [u'prop_name', u'value']

  def __init__(self, project_id, namespace, path, prop_name, value, commit_vs,
               deleted_vs):
    super(PropertyEntry, self).__init__(
      project_id, namespace, path, commit_vs, deleted_vs)
    self.prop_name = prop_name
    self.value = value

  def __repr__(self):
    return u'PropertyEntry(%r, %r, %r, %r, %r, %r, %r)' % (
      self.project_id, self.namespace, self.path, self.prop_name, self.value,
      self.commit_vs, self.deleted_vs)

  def prop_result(self):
    entity = entity_pb.EntityProto()
    entity.mutable_key().MergeFrom(self.key)
    entity.mutable_entity_group().MergeFrom(self.group)
    prop = entity.add_property()
    prop.set_name(self.prop_name)
    prop.set_meaning(entity_pb.Property.INDEX_VALUE)
    prop.set_multiple(False)
    prop.mutable_value().MergeFrom(self.value)
    return entity

  def cursor_result(self):
    compiled_cursor = datastore_pb.CompiledCursor()
    position = compiled_cursor.add_position()
    position.mutable_key().MergeFrom(self.key)
    position.set_start_inclusive(False)
    index_value = position.add_indexvalue()
    index_value.set_property(self.prop_name)
    index_value.mutable_value().MergeFrom(self.value)
    return compiled_cursor


class CompositeEntry(IndexEntry):
  __SLOTS__ = [u'properties']

  def __init__(self, project_id, namespace, path, properties, commit_vs,
               deleted_vs):
    super(CompositeEntry, self).__init__(
      project_id, namespace, path, commit_vs, deleted_vs)
    self.properties = properties

  def __repr__(self):
    return u'CompositeEntry(%r, %r, %r, %r, %r, %r)' % (
      self.project_id, self.namespace, self.path, self.properties,
      self.commit_vs, self.deleted_vs)

  def prop_result(self):
    entity = entity_pb.EntityProto()
    entity.mutable_key().MergeFrom(self.key)
    entity.mutable_entity_group().MergeFrom(self.group)
    for prop_name, value in self.properties:
      prop = entity.add_property()
      prop.set_name(prop_name)
      prop.set_meaning(entity_pb.Property.INDEX_VALUE)
      # TODO: Check if this is sometimes True.
      prop.set_multiple(False)
      prop.mutable_value().MergeFrom(value)

    return entity

  def cursor_result(self):
    compiled_cursor = datastore_pb.CompiledCursor()
    position = compiled_cursor.add_position()
    position.mutable_key().MergeFrom(self.key)
    position.set_start_inclusive(False)
    for prop_name, value in self.properties:
      index_value = position.add_indexvalue()
      index_value.set_property(prop_name)
      index_value.mutable_value().MergeFrom(value)

    return compiled_cursor


class IndexIterator(object):
  def __init__(self, tr, tornado_fdb, index, key_slice, fetch_limit, reverse,
               read_vs=None, snapshot=False):
    self.index = index
    if isinstance(key_slice.start, fdb.KeySelector):
      logger.debug('start: {!r}'.format(key_slice.start.key))
    else:
      logger.debug('start: {!r}'.format(key_slice.start))

    if isinstance(key_slice.stop, fdb.KeySelector):
      logger.debug('stop: {!r}'.format(key_slice.stop.key))
    else:
      logger.debug('stop: {!r}'.format(key_slice.stop))

    self._kv_iterator = KVIterator(
      tr, tornado_fdb, key_slice, fetch_limit, reverse, snapshot=snapshot)
    if read_vs is None:
      read_vs = fdb.tuple.Versionstamp()

    self._read_vs = read_vs
    self._done = False

  @property
  def prop_names(self):
    return self.index.prop_names

  @property
  def start_key(self):
    return self._kv_iterator.slice.start.key

  @gen.coroutine
  def next_page(self):
    if self._done:
      raise gen.Return(([], False))

    kvs, more_results = yield self._kv_iterator.next_page()
    usable_entries = []
    for kv in kvs:
      entry = self.index.decode(kv)
      if not entry.commit_vs < self._read_vs <= entry.deleted_vs:
        self._kv_iterator.increase_limit()
        more_results = not self._kv_iterator.done_with_range
        continue

      usable_entries.append(entry)

    if not more_results:
      self._done = True

    raise gen.Return((usable_entries, more_results))


class MultipleRangeIterator(object):
  def __init__(self, iterators, fetch_limit):
    self._iterators = sorted(
      iterators, key=lambda iterator: iterator.start_key)
    self._fetch_limit = fetch_limit
    self._fetched = 0
    self._done = False

  @gen.coroutine
  def next_page(self):
    if self._done:
      raise gen.Return(([], False))

    entries, more_results = yield self._iterators[0].next_page()
    if not more_results:
      self._iterators.pop(0)

    remaining = self._fetch_limit - self._fetched
    entries = entries[:remaining]
    self._fetched += len(entries)
    if self._fetched == self._fetch_limit or not self._iterators:
      self._done = True

    raise gen.Return((entries, not self._done))


class MergeJoinIterator(object):
  def __init__(self, tr, tornado_fdb, filter_props, indexes, fetch_limit,
               read_vs=None, snapshot=False):
    self.indexes = indexes
    if read_vs is None:
      read_vs = fdb.tuple.Versionstamp()

    self._filter_props = filter_props
    self._read_vs = read_vs
    self._tr = tr
    self._tornado_fdb = tornado_fdb
    self._fetch_limit = fetch_limit
    self._fetched = 0
    self._snapshot = snapshot
    self._done = False
    self._candidate_path = None
    self._candidate_entries = []

  @property
  def prop_names(self):
    prop_names = set()
    for index, _, _, _ in self.indexes:
      prop_names.update(index.prop_names)

    return tuple(prop_names)

  @gen.coroutine
  def next_page(self):
    if self._done:
      raise gen.Return(([], False))

    result = None
    for i, (index, key_slice, prop_name, value) in enumerate(self.indexes):
      logger.debug('prop_name: {}, value: {!r}'.format(prop_name, encode_value(value)))
      usable_entry = None
      # TODO: Keep cache of ranges to reduce unnecessary lookups.
      index_exhausted = False
      while True:
        kvs, count, more = yield self._tornado_fdb.get_range(
          self._tr, key_slice, 0, fdb.StreamingMode.small, 1,
          snapshot=self._snapshot)
        if not count:
          logger.debug('no more results')
          index_exhausted = True
          break

        key_slice = slice(fdb.KeySelector.first_greater_than(kvs[-1].key),
                          key_slice.stop)
        for kv in kvs:
          entry = index.decode(kv)
          if entry.commit_vs < self._read_vs <= entry.deleted_vs:
            usable_entry = entry
            break

        if usable_entry is not None:
          break

      if index_exhausted:
        self._done = True
        break

      logger.debug('usable entry: {}'.format(usable_entry))
      if usable_entry.path == self._candidate_path:
        self._candidate_entries.append(usable_entry)
      else:
        self._candidate_path = usable_entry.path
        self._candidate_entries = [usable_entry]

      next_index_op = Query_Filter.GREATER_THAN_OR_EQUAL
      if len(self._candidate_entries) == len(self.indexes):
        properties = []
        for partial_entry in self._candidate_entries:
          if isinstance(partial_entry, PropertyEntry):
            properties.append((partial_entry.prop_name, partial_entry.value))
          else:
            for property in partial_entry.properties:
              if property not in properties:
                properties.append(property)

        result = CompositeEntry(
          usable_entry.project_id, usable_entry.namespace,
          self._candidate_path, properties, usable_entry.commit_vs,
          usable_entry.deleted_vs)
        self._candidate_entries = []
        next_index_op = Query_Filter.GREATER_THAN

      next_index_i = (i + 1) % len(self.indexes)
      next_index, next_slice, next_prop_name, next_value =\
        self.indexes[next_index_i]

      # TODO: This probably doesn't work for all cases.
      last_prop = None
      if isinstance(next_index, CompositeIndex):
        last_prop = next_index.prop_names[-1]

      tmp_filter_props = []
      for filter_prop in self._filter_props:
        if (filter_prop.name in (next_prop_name, last_prop, KEY_PROP) or
            filter_prop.name not in next_index.prop_names):
          continue
        else:
          tmp_filter_props.append(filter_prop)

      tmp_filter_props.append(
        FilterProperty(next_prop_name, [(Query_Filter.EQUAL, next_value)]))

      if last_prop is not None:
        val = next(value for prop_name, value in usable_entry.properties
                   if prop_name == last_prop)
        tmp_filter_props.append(
          FilterProperty(last_prop, [(Query_Filter.EQUAL, val)]))

      tmp_filter_props.append(
        FilterProperty(KEY_PROP, [(next_index_op, usable_entry.path)]))

      new_slice = next_index.get_slice(tmp_filter_props)
      encoded_value = encode_value(next_value)
      logger.debug('changing {} from {!r} to {!r}'.format(encoded_value, next_slice.start.key, new_slice.start.key))
      self.indexes[next_index_i][1] = new_slice

    results = [result] if result is not None else []
    self._fetched += len(results)
    if self._fetched == self._fetch_limit:
      logger.debug('done with limit')
      self._done = True

    raise gen.Return((results, not self._done))


class Index(object):
  __SLOTS__ = [u'directory']

  def __init__(self, directory):
    self.directory = directory

  @property
  def project_id(self):
    return self.directory.get_path()[2]

  @property
  def namespace(self):
    return self.directory.get_path()[4]

  @property
  def prop_names(self):
    return tuple()

  def pack_method(self, versionstamp):
    if versionstamp.is_complete():
      return self.directory.pack
    else:
      return self.directory.pack_with_versionstamp

  def encode_path(self, path):
    raise NotImplementedError()

  def get_slice(self, filter_props, ancestor_path=tuple(), cursor=None,
                reverse=False):
    subspace = self.directory
    start = None
    stop = None
    if ancestor_path:
      start, stop = encode_ancestor_range(subspace, ancestor_path)

    for filter_prop in filter_props:
      if filter_prop.name != KEY_PROP:
        raise BadRequest(u'Unexpected filter: {}'.format(filter_prop.name))

      if filter_prop.equality:
        encoded_path = self.encode_path(filter_prop.filters[0][1])
        subspace = subspace.subspace((encoded_path,))
        continue

      for op, value in filter_prop.filters:
        encoded_path = self.encode_path(value)
        if op in START_FILTERS:
          start = get_fdb_key_selector(op, subspace.pack((encoded_path,)))
        elif op in STOP_FILTERS:
          stop = get_fdb_key_selector(op, subspace.pack((encoded_path,)))
        else:
          raise BadRequest(u'Unexpected filter operation: {}'.format(op))

    if cursor is not None:
      encoded_path = self.encode_path(cursor.key().path())
      if reverse:
        stop = get_fdb_key_selector(Query_Filter.LESS_THAN,
                                    subspace.pack((encoded_path,)))
      else:
        start = get_fdb_key_selector(Query_Filter.GREATER_THAN,
                                     subspace.pack((encoded_path,)))

    selector = fdb.KeySelector.first_greater_or_equal
    start = start or selector(subspace.range().start)
    stop = stop or selector(subspace.range().stop)
    return slice(start, stop)


class KindlessIndex(Index):
  DIR_NAME = u'kindless'

  @classmethod
  def from_cache(cls, project_id, namespace, directory_cache):
    directory = directory_cache.get(
      (project_id, INDEX_DIR, namespace, cls.DIR_NAME))
    return cls(directory)

  def __repr__(self):
    return u'KindlessIndex(%r)' % self.directory

  def encode_path(self, path):
    if not isinstance(path, tuple):
      path = encode_path(path)

    return path

  def encode(self, path, commit_vs):
    return self.pack_method(commit_vs)((path, commit_vs))

  def decode(self, kv):
    path, commit_vs = self.directory.unpack(kv.key)
    deleted_vs = None
    if kv.value:
      deleted_vs = fdb.tuple.Versionstamp(kv.value)

    return IndexEntry(self.project_id, self.namespace, path, commit_vs,
                      deleted_vs)


class KindIndex(Index):
  DIR_NAME = u'kind'

  @classmethod
  def from_cache(cls, project_id, namespace, kind, directory_cache):
    directory = directory_cache.get(
      (project_id, INDEX_DIR, namespace, cls.DIR_NAME, kind))
    return cls(directory)

  @property
  def kind(self):
    return self.directory.get_path()[-1]

  def __repr__(self):
    return u'KindIndex(%r)' % self.directory

  def encode_path(self, path):
    if not isinstance(path, tuple):
      path = encode_path(path)

    kindless_path = path[:-2] + path[-1:]
    return kindless_path

  def encode(self, path, commit_vs):
    return self.pack_method(commit_vs)((self.encode_path(path), commit_vs))

  def decode(self, kv):
    kindless_path, commit_vs = self.directory.unpack(kv.key)
    path = kindless_path[:-1] + (self.kind,) + kindless_path[-1:]
    deleted_vs = None
    if kv.value:
      deleted_vs = fdb.tuple.Versionstamp(kv.value)

    return IndexEntry(self.project_id, self.namespace, path, commit_vs,
                      deleted_vs)


class SinglePropIndex(Index):
  DIR_NAME = u'single-property'

  @classmethod
  def from_cache(cls, project_id, namespace, kind, prop_name, directory_cache):
    directory = directory_cache.get(
      (project_id, INDEX_DIR, namespace, cls.DIR_NAME, kind, prop_name))
    return cls(directory)

  @property
  def kind(self):
    return self.directory.get_path()[-2]

  @property
  def prop_name(self):
    return self.directory.get_path()[-1]

  @property
  def prop_names(self):
    return (self.directory.get_path()[-1],)

  def __repr__(self):
    return u'SinglePropIndex(%r)' % self.directory

  def encode_path(self, path):
    if not isinstance(path, tuple):
      path = encode_path(path)

    kindless_path = path[:-2] + path[-1:]
    return kindless_path

  def encode(self, value, path, commit_vs):
    return self.pack_method(commit_vs)(
      (encode_value(value), self.encode_path(path), commit_vs))

  def decode(self, kv):
    encoded_value, kindless_path, commit_vs = self.directory.unpack(kv.key)
    value = decode_value(encoded_value)
    path = kindless_path[:-1] + (self.kind,) + kindless_path[-1:]
    deleted_vs = None
    if kv.value:
      deleted_vs = fdb.tuple.Versionstamp(kv.value)

    return PropertyEntry(self.project_id, self.namespace, path, self.prop_name,
                         value, commit_vs, deleted_vs)

  def get_slice(self, filter_props, ancestor_path=tuple(), cursor=None,
                reverse=False):
    subspace = self.directory
    start = None
    stop = None
    if ancestor_path:
      # Apply property equality first if it exists.
      if filter_props and filter_props[0].name == self.prop_name:
        if not filter_props[0].equality:
          raise BadRequest(u'Invalid index for ancestor query')

        value = filter_props[0].filters[0][1]
        subspace = subspace.subspace((encode_value(value),))
        filter_props = filter_props[1:]

      start, stop = encode_ancestor_range(subspace, ancestor_path)

    for filter_prop in filter_props:
      if filter_prop.name == self.prop_name:
        encoder = encode_value
      elif filter_prop.name == KEY_PROP:
        encoder = self.encode_path
      else:
        raise BadRequest(u'Unexpected filter: {}'.format(filter_prop.name))

      if filter_prop.equality:
        encoded_value = encoder(filter_prop.filters[0][1])
        subspace = subspace.subspace((encoded_value,))
        continue

      for op, value in filter_prop.filters:
        encoded_value = encoder(value)
        if op in START_FILTERS:
          start = get_fdb_key_selector(op, subspace.pack((encoded_value,)))
        elif op in STOP_FILTERS:
          stop = get_fdb_key_selector(op, subspace.pack((encoded_value,)))
        else:
          raise BadRequest(u'Unexpected filter operation: {}'.format(op))

    if cursor is not None:
      prop = next(prop for prop in cursor.property_list()
                  if prop.name() == self.prop_name)
      encoded_value = encode_value(prop.value())
      encoded_path = self.encode_path(cursor.key().path())
      encoded_cursor = (encoded_value, encoded_path)
      if reverse:
        stop = get_fdb_key_selector(Query_Filter.LESS_THAN,
                                    subspace.pack(encoded_cursor))
      else:
        start = get_fdb_key_selector(Query_Filter.GREATER_THAN,
                                     subspace.pack(encoded_cursor))

    selector = fdb.KeySelector.first_greater_or_equal
    start = start or selector(subspace.range().start)
    stop = stop or selector(subspace.range().stop)
    return slice(start, stop)


class CompositeIndex(Index):
  __SLOTS__ = [u'kind', u'ancestor', u'order_info']

  DIR_NAME = u'composite'

  def __init__(self, directory, kind, ancestor, order_info):
    super(CompositeIndex, self).__init__(directory)
    self.kind = kind
    self.ancestor = ancestor
    self.order_info = order_info

  @property
  def id(self):
    return int(self.directory.get_path()[6])

  @property
  def prop_names(self):
    return tuple(prop_name for prop_name, _ in self.order_info)

  @classmethod
  def from_cache(cls, project_id, namespace, index_id, kind, ancestor,
                 order_info, directory_cache):
    directory = directory_cache.get(
      (project_id, INDEX_DIR, namespace, cls.DIR_NAME,
       six.text_type(index_id)))
    return cls(directory, kind, ancestor, order_info)

  def __repr__(self):
    return u'CompositeIndex(%r, %r, %r, %r)' % (
      self.directory, self.kind, self.ancestor, self.order_info)

  def encode_path(self, path):
    if not isinstance(path, tuple):
      path = encode_path(path)

    kindless_path = path[:-2] + path[-1:]
    return kindless_path

  def encode(self, prop_list, path, commit_vs):
    encoded_values_by_prop = []
    for index_prop_name, direction in self.order_info:
      reverse = direction == Query_Order.DESCENDING
      encoded_values_by_prop.append(
        tuple(encode_value(prop.value(), reverse) for prop in prop_list
              if prop.name() == index_prop_name))

    pack = self.pack_method(commit_vs)
    encoded_value_combos = itertools.product(*encoded_values_by_prop)
    if not self.ancestor:
      return tuple(pack(values + (self.encode_path(path),) + (commit_vs,))
                   for values in encoded_value_combos)

    keys = []
    for index in range(2, len(path), 2):
      ancestor_path = path[:index]
      remaining_path = self.encode_path(path[index:])
      keys.extend(
        [pack((ancestor_path,) + values + (remaining_path,) + (commit_vs,))
         for values in encoded_value_combos])

    return tuple(keys)

  def decode(self, kv):
    unpacked_key = self.directory.unpack(kv.key)
    if self.ancestor:
      kindless_path = unpacked_key[0] + unpacked_key[-2]
      values = unpacked_key[1:-2]
    else:
      kindless_path = unpacked_key[-2]
      values = unpacked_key[:-2]

    properties = []
    for index, prop_name in enumerate(self.prop_names):
      properties.append((prop_name, decode_value(values[index])))

    path = kindless_path[:-1] + (self.kind,) + kindless_path[-1:]
    commit_vs = unpacked_key[-1]
    deleted_vs = None
    if kv.value:
      deleted_vs = fdb.tuple.Versionstamp(kv.value)

    return CompositeEntry(self.project_id, self.namespace, path, properties,
                          commit_vs, deleted_vs)

  def get_slice(self, filter_props, ancestor_path=tuple(), cursor=None,
                reverse=False):
    subspace = self.directory
    if ancestor_path:
      subspace = subspace.subspace((ancestor_path,))

    start = None
    stop = None

    ordered_filter_props = []
    for prop_name in self.prop_names + (KEY_PROP,):
      logger.debug('prop_name: {}'.format(prop_name))
      try:
        filter_prop = next(filter_prop for filter_prop in filter_props
                           if filter_prop.name == prop_name)
        ordered_filter_props.append(filter_prop)
      except StopIteration:
        continue

    for filter_prop in ordered_filter_props:
      logger.debug('processing {}'.format(filter_prop.name))
      index_direction = next(
        (direction for name, direction in self.order_info
         if name == filter_prop.name), Query_Order.ASCENDING)
      reverse = index_direction == Query_Order.DESCENDING
      if filter_prop.name in self.prop_names:
        encoder = lambda val: encode_value(val, reverse)
      elif filter_prop.name == KEY_PROP:
        encoder = self.encode_path
      else:
        raise BadRequest(u'Unexpected filter: {}'.format(filter_prop.name))

      if filter_prop.equality:
        encoded_value = encoder(filter_prop.filters[0][1])
        logger.debug('encoded_value: {}'.format(encoded_value))
        subspace = subspace.subspace((encoded_value,))
        continue

      logger.debug('filters: {}'.format(filter_prop.filters))
      for op, value in filter_prop.filters:
        if filter_prop.name == KEY_PROP:
          encoded_value = self.encode_path(value)
        else:
          encoded_value = encoder(value)

        logger.debug('value: {}'.format(value))
        logger.debug('encoded value: {}'.format(encoded_value))
        selector = get_fdb_key_selector(op, subspace.pack((encoded_value,)))
        if ((op in START_FILTERS and not reverse) or
            (op in STOP_FILTERS and reverse)):
          start = selector
        elif ((op in STOP_FILTERS and not reverse) or
              (op in START_FILTERS and reverse)):
          stop = get_fdb_key_selector(op, subspace.pack((encoded_value,)))
        else:
          raise BadRequest(u'Unexpected filter operation: {}'.format(op))

    if cursor is not None:
      full_path = encode_path(cursor.key().path())
      remaining_path = self.encode_path(full_path[len(ancestor_path):])
      encoded_values = []
      for prop_name, index_direction in self.order_info:
        prop = next(prop for prop in cursor.property_list()
                    if prop.name() == prop_name)
        reverse = index_direction == Query_Order.DESCENDING
        encoded_values.append(encode_value(prop.value(), reverse))

      encoded_cursor = ((ancestor_path,) + tuple(encoded_values) +
                        (remaining_path,))
      if reverse:
        stop = get_fdb_key_selector(Query_Filter.LESS_THAN,
                                    subspace.pack(encoded_cursor))
      else:
        start = get_fdb_key_selector(Query_Filter.GREATER_THAN,
                                     subspace.pack(encoded_cursor))

    selector = fdb.KeySelector.first_greater_or_equal
    start = start or selector(subspace.range().start)
    stop = stop or selector(subspace.range().stop)
    return slice(start, stop)


class IndexManager(object):
  _MAX_RESULTS = 300

  def __init__(self, db, directory_cache, tornado_fdb, data_manager):
    self.composite_index_manager = None
    self._db = db
    self._directory_cache = directory_cache
    self._tornado_fdb = tornado_fdb
    self._data_manager = data_manager

  def put_entries(self, tr, old_entity, old_vs, new_entity):
    if old_entity is not None:
      for key in self._get_index_keys(old_entity, old_vs):
        tr.set_versionstamped_value(key, b'\x00' * 14)

    if new_entity is not None:
      for key in self._get_index_keys(new_entity):
        tr.set_versionstamped_key(key, b'')

  def rpc_limit(self, query):
    check_more_results = False
    limit = None
    if query.has_limit():
      limit = query.limit()

    if query.has_count() and (limit is None or limit > query.count()):
      check_more_results = True
      limit = query.count()

    if limit is None or limit > self._MAX_RESULTS:
      check_more_results = True
      limit = self._MAX_RESULTS

    if query.has_offset():
      limit += query.offset()

    return limit, check_more_results

  def include_data(self, query):
    if query.keys_only() and query.property_name_list():
      raise BadRequest(
        u'A keys-only query cannot include a property name list')

    if query.keys_only():
      return False

    if not query.property_name_list():
      return True

    return False

  def get_iterator(self, tr, query, read_vs=None):
    project_id = decode_str(query.app())
    namespace = decode_str(query.name_space())
    index = self._get_perfect_index(query)
    filter_props = group_filters(query)
    ancestor_path = tuple()
    if query.has_ancestor():
      ancestor_path = encode_path(query.ancestor().path())

    reverse = get_scan_direction(query, index) == Query_Order.DESCENDING
    last_result = None
    if query.has_compiled_cursor():
      cursor = appscale_stub_util.ListCursor(query)
      last_result = cursor._GetLastResult()

    rpc_limit, check_more_results = self.rpc_limit(query)
    fetch_limit = rpc_limit
    if check_more_results:
      fetch_limit += 1

    if index is None:
      if not all(prop.equality for prop in filter_props):
        raise BadRequest(u'Query not supported')

      indexes = []
      for filter_prop in filter_props:
        index = SinglePropIndex.from_cache(
          project_id, namespace, decode_str(query.kind()), filter_prop.name,
          self._directory_cache)
        for op, value in filter_prop.filters:
          tmp_filter_prop = FilterProperty(filter_prop.name, [(op, value)])
          slice = index.get_slice((tmp_filter_prop,), ancestor_path,
                                  last_result)
          indexes.append([index, slice, filter_prop.name, value])

      return MergeJoinIterator(tr, self._tornado_fdb, filter_props, indexes,
                               fetch_limit, read_vs, snapshot=True)

    logger.debug('using index: {}'.format(index))
    equality_prop = next(
      (filter_prop for filter_prop in filter_props if filter_prop.equality),
      None)
    if equality_prop is not None and len(equality_prop.filters) > 1:
      indexes = []
      for op, value in equality_prop.filters:
        tmp_filter_props = []
        for filter_prop in filter_props:
          if filter_prop.name == equality_prop.name:
            tmp_filter_props.append(
              FilterProperty(filter_prop.name, [(op, value)]))
          else:
            tmp_filter_props.append(filter_prop)

        desired_slice = index.get_slice(
          tmp_filter_props, ancestor_path, last_result, reverse)
        indexes.append([index, desired_slice, equality_prop.name, value])

      return MergeJoinIterator(tr, self._tornado_fdb, filter_props, indexes,
                               fetch_limit, read_vs, snapshot=True)

    desired_slice = index.get_slice(filter_props, ancestor_path, last_result,
                                    reverse)

    iterator = IndexIterator(tr, self._tornado_fdb, index, desired_slice,
                             fetch_limit, reverse, read_vs, snapshot=True)

    return iterator

  def _get_index_keys(self, entity, commit_vs=None):
    if commit_vs is None:
      commit_vs = fdb.tuple.Versionstamp()

    project_id = decode_str(entity.key().app())
    namespace = decode_str(entity.key().name_space())
    path = encode_path(entity.key().path())
    kind = path[-2]

    kindless_index = KindlessIndex.from_cache(
      project_id, namespace, self._directory_cache)
    kind_index = KindIndex.from_cache(
      project_id, namespace, kind, self._directory_cache)
    composite_indexes = self._get_indexes(project_id, namespace, kind)

    all_keys = [kindless_index.encode(path, commit_vs),
                kind_index.encode(path, commit_vs)]
    entity_prop_names = []
    for prop in entity.property_list():
      prop_name = decode_str(prop.name())
      entity_prop_names.append(prop_name)
      index = SinglePropIndex.from_cache(
        project_id, namespace, kind, prop_name, self._directory_cache)
      all_keys.append(index.encode(prop.value(), path, commit_vs))

    for index in composite_indexes:
      if not all(index_prop_name in entity_prop_names
                 for index_prop_name in index.prop_names):
        continue

      all_keys.extend(index.encode(entity.property_list(), path, commit_vs))

    return all_keys

  def _get_perfect_index(self, query):
    project_id = decode_str(query.app())
    namespace = decode_str(query.name_space())
    filter_props = group_filters(query)
    order_info = get_order_info(query)

    prop_names = [filter_prop.name for filter_prop in filter_props]
    prop_names.extend([prop_name for prop_name, _ in order_info
                       if prop_name not in prop_names])
    prop_names.extend([decode_str(prop_name)
                       for prop_name in query.property_name_list()
                       if prop_name not in prop_names])

    if not query.has_kind():
      if not all(prop_name == KEY_PROP for prop_name in prop_names):
        raise BadRequest(u'kind must be specified when filtering or ordering '
                         u'properties other than __key__')

      return KindlessIndex.from_cache(
        project_id, namespace, self._directory_cache)

    kind = decode_str(query.kind())
    if all(prop_name == KEY_PROP for prop_name in prop_names):
      return KindIndex.from_cache(
        project_id, namespace, kind, self._directory_cache)

    if sum(prop_name != KEY_PROP for prop_name in prop_names) == 1:
      prop_name = next(prop_name for prop_name in prop_names
                       if prop_name != KEY_PROP)
      ordered_prop = prop_name in [order_name for order_name, _ in order_info]
      if not query.has_ancestor() or not ordered_prop:
        return SinglePropIndex.from_cache(
          project_id, namespace, decode_str(query.kind()), prop_name,
          self._directory_cache)

    index_pb = _FindIndexToUse(query, self._get_indexes_pb(project_id))
    if index_pb is not None:
      index_order_info = tuple(
        (decode_str(prop.name()), prop.direction())
        for prop in index_pb.definition().property_list())
      return CompositeIndex.from_cache(
        project_id, namespace, index_pb.id(), kind,
        index_pb.definition().ancestor(), index_order_info,
        self._directory_cache)

    return None

  def _get_indexes(self, project_id, namespace, kind):
    try:
      project_index_manager = self.composite_index_manager.projects[project_id]
    except KeyError:
      raise BadRequest(u'project_id: {} not found'.format(project_id))

    relevant_indexes = [index for index in project_index_manager.indexes
                        if index.kind == kind]
    fdb_indexes = []
    for index in relevant_indexes:
      order_info = []
      for prop in index.properties:
        direction = (Query_Order.DESCENDING if prop.direction == 'desc'
                     else Query_Order.ASCENDING)
        order_info.append((prop.name, direction))

      fdb_indexes.append(CompositeIndex.from_cache(
        project_id, namespace, index.id, index.kind, index.ancestor,
        order_info, self._directory_cache))

    return fdb_indexes

  def _get_indexes_pb(self, project_id):
    try:
      project_index_manager = self.composite_index_manager.projects[project_id]
    except KeyError:
      raise BadRequest(u'project_id: {} not found'.format(project_id))

    try:
      indexes = project_index_manager.indexes_pb
    except IndexInaccessible:
      raise InternalError(u'ZooKeeper is not accessible')

    return indexes

  @gen.coroutine
  def update_composite_index(self, project_id, index_pb, cursor=(None, None)):
    start_ns, start_key = cursor
    kind = decode_str(index_pb.definition().entity_type())
    ancestor = index_pb.definition().ancestor()
    order_info = ((prop.name(), prop.direction())
                  for prop in index_pb.definition().property_list())
    indexes_dir = self._directory_cache.get((project_id, INDEX_DIR))
    tr = self._db.create_transaction()
    deadline = time.time() + MAX_FDB_TX_DURATION / 2
    for namespace in indexes_dir.list(tr):
      if start_ns is not None and namespace < start_ns:
        continue

      u_index_id = six.text_type(index_pb.id())
      composite_index_dir = indexes_dir.create_or_open(
        tr, (namespace, CompositeIndex.DIR_NAME, u_index_id))
      composite_index = CompositeIndex(composite_index_dir, kind, ancestor,
                                       order_info)
      logger.info(u'Backfilling {}'.format(composite_index))
      try:
        kind_index_dir = indexes_dir.open(
          tr, (namespace, KindIndex.DIR_NAME, kind))
      except ValueError:
        logger.info(u'No entities exist for {}'.format(composite_index))
        continue

      kind_index = KindIndex(kind_index_dir)
      remaining_range = kind_index_dir.range()
      if start_key is not None:
        remaining_range = slice(
          fdb.KeySelector.first_greater_than(start_key), remaining_range.stop)
        start_key = None

      kv_iterator = KVIterator(tr, self._tornado_fdb, remaining_range)
      while True:
        kvs, more_results = yield kv_iterator.next_page()
        entries = [kind_index.decode(kv) for kv in kvs]
        entity_results = yield [self._data_manager.get_entry(self, tr, entry)
                                for entry in entries]
        for index, kv in enumerate(kvs):
          entity = entity_pb.EntityProto(entity_results[index][1])
          entry = entries[index]
          keys = composite_index.encode(
            entity.property_list(), entry.path, entry.commit_vs)
          for key in keys:
            deleted_val = (entry.deleted_vs.to_bytes()
                           if entry.deleted_vs.is_complete() else b'')
            tr[key] = deleted_val

        if not more_results:
          logger.info(u'Finished backfilling {}'.format(composite_index))
          break

        if time.time() > deadline:
          try:
            yield self._tornado_fdb.commit(tr)
            cursor = (namespace, kvs[-1].key)
          except fdb.FDBError as fdb_error:
            logger.warning(u'Error while updating index: {}'.format(fdb_error))
            tr.on_error(fdb_error).wait()

          yield self.update_composite_index(project_id, index_pb, cursor)
          return
