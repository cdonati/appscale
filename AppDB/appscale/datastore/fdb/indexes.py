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
from appscale.datastore.fdb.utils import (
  decode_path, fdb, flat_path, MAX_FDB_TX_DURATION, RangeIterator)
from appscale.datastore.dbconstants import BadRequest, InternalError
from appscale.datastore.index_manager import IndexInaccessible
from appscale.datastore.utils import _FindIndexToUse

sys.path.append(APPSCALE_PYTHON_APPSERVER)
from google.appengine.datastore import datastore_pb, entity_pb
from google.appengine.datastore.datastore_pb import Query_Filter, Query_Order

logger = logging.getLogger(__name__)

INDEX_DIR = u'indexes'

KEY_PROP = u'__key__'


class V3Types(object):
  NULL = b'0'
  INT64 = b'1'
  BOOLEAN = b'2'
  STRING = b'3'
  DOUBLE = b'4'
  POINT = b'5'
  USER = b'6'
  REFERENCE = b'7'


COMPOUND_TYPES = (V3Types.POINT, V3Types.USER, V3Types.REFERENCE)

# Signifies the end of an encoded reference value.
REF_VAL_DELIMETER = b'\x00'

START_FILTERS = (Query_Filter.GREATER_THAN_OR_EQUAL, Query_Filter.GREATER_THAN)
STOP_FILTERS = (Query_Filter.LESS_THAN_OR_EQUAL, Query_Filter.LESS_THAN)


def get_type(value):
  readable_types = [
    (name.lower(), encoded) for name, encoded in V3Types.__dict__.items()
    if not name.startswith('_') and encoded != V3Types.NULL]
  for type_name, encoded_type in readable_types:
    if getattr(value, 'has_{}value'.format(type_name))():
      return type_name, encoded_type

  return None, V3Types.NULL


def get_type_name(encoded_type):
  return next(key for key, val in V3Types.__dict__.items()
              if val == encoded_type).lower()


def encode_value(value):
  type_name, encoded_type = get_type(value)
  if encoded_type == V3Types.NULL:
    return (encoded_type,)

  if encoded_type not in COMPOUND_TYPES:
    encoded_value = getattr(value, '{}value'.format(type_name))()
    if encoded_type == V3Types.STRING:
      encoded_value = six.text_type(encoded_value)

    return encoded_type, encoded_value

  if encoded_type == V3Types.POINT:
    return encoded_type, value.x(), value.y()

  if encoded_type == V3Types.USER:
    email = six.text_type(value.email())
    auth_domain = six.text_type(value.auth_domain())
    return encoded_type, email, auth_domain

  if encoded_type == V3Types.REFERENCE:
    project_id = six.text_type(value.app())
    namespace = six.text_type(value.name_space())
    encoded_value = (project_id, namespace) + flat_path(value)
    return (encoded_type,) + encoded_value + (REF_VAL_DELIMETER,)

  raise BadRequest(u'{} is not a supported value'.format(type_name))


def pop_value(unpacked_key):
  value = entity_pb.PropertyValue()
  encoded_type = unpacked_key[0]
  if encoded_type == V3Types.NULL:
    return value, unpacked_key[1:]

  if encoded_type not in COMPOUND_TYPES:
    type_name = get_type_name(encoded_type)
    getattr(value, 'set_{}value'.format(type_name))(unpacked_key[1])
    return value, unpacked_key[2:]

  if encoded_type == V3Types.POINT:
    point_val = value.mutable_pointvalue()
    point_val.set_x(unpacked_key[1])
    point_val.set_y(unpacked_key[2])
    return value, unpacked_key[3:]

  if encoded_type == V3Types.USER:
    user_val = value.mutable_uservalue()
    user_val.set_email(unpacked_key[1])
    user_val.set_email(unpacked_key[2])
    return value, unpacked_key[3:]

  if encoded_type == V3Types.REFERENCE:
    delimiter_index = unpacked_key.index(REF_VAL_DELIMETER)
    value_parts = unpacked_key[1:delimiter_index]
    reference_val = value.mutable_referencevalue()
    reference_val.set_app(value_parts[0])
    reference_val.set_name_space(value_parts[1])
    reference_val.MergeFrom(
      decode_path(value_parts[2:], reference_value=True))
    return value, unpacked_key[slice(delimiter_index + 1, None)]

  raise InternalError(u'Unsupported PropertyValue type')


def group_filters(query):
  filter_props = []
  for query_filter in query.filter_list():
    if query_filter.property_size() != 1:
      raise BadRequest(u'Each filter must have exactly one property')

    prop = query_filter.property(0)
    prop_name = six.text_type(prop.name())
    filter_info = (query_filter.op(), prop.value())
    if filter_props and prop_name == filter_props[1][0]:
      filter_props[1][1].append(filter_info)
    else:
      filter_props.append((prop_name, [filter_info]))

  for name, filters in filter_props[:-1]:
    if name == KEY_PROP:
      raise BadRequest(
        u'Only the last filter property can be on {}'.format(KEY_PROP))

    if len(filters) != 1 or filters[0][0] != Query_Filter.EQUAL:
      raise BadRequest(u'All but the last property must be equality filters')

  if len(filter_props[-1][1]) > 2:
    raise BadRequest(u'A property can only have up to two filters')

  return tuple(filter_props)


def get_order_info(query):
  filter_props = group_filters(query)

  # Orders on equality filters can be ignored.
  equality_props = [prop_name for prop_name, filters in filter_props
                    if filters[0][0] == Query_Filter.EQUAL]
  relevant_orders = [order for order in query.order_list()
                     if order.property() not in equality_props]

  order_info = []
  for prop_name, filters in filter_props:
    direction = next(
      (order.direction() for order in relevant_orders
       if order.property() == prop_name), Query_Order.ASCENDING)
    order_info.append((prop_name, direction))

  filter_prop_names = [prop_name for prop_name, _ in filter_props]
  order_info.extend(
    [(order.property(), order.direction()) for order in relevant_orders
     if order.property() not in filter_prop_names])

  return tuple(order_info)


def key_selector(op):
  """ Like Python's slice notation, FDB range queries include the start and
      exclude the stop. Therefore, the stop selector must point to the first
      key that will be excluded from the results. """
  if op in (Query_Filter.GREATER_THAN_OR_EQUAL, Query_Filter.LESS_THAN):
    return fdb.KeySelector.first_greater_or_equal
  elif op in (Query_Filter.GREATER_THAN, Query_Filter.LESS_THAN_OR_EQUAL):
    return fdb.KeySelector.first_greater_than
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
    element = group.add_element()
    element.set_type(self.path[0])
    if isinstance(self.path[1], int):
      element.set_id(self.path[1])
    else:
      element.set_name(self.path[1])

    return group

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

  def prop_result(self):
    entity = entity_pb.EntityProto()
    entity.mutable_key().MergeFrom(self.key)
    entity.mutable_entity_group().MergeFrom(self.group)
    for prop_name, value in self.properties:
      prop = entity.add_property()
      prop.set_name(prop_name)
      prop.set_meaning(entity_pb.Property.INDEX_VALUE)
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
  def __init__(self, tr, tornado_fdb, desired_slice, limit, reverse, index,
               ancestor=None, read_vs=None):
    self.index = index
    self._read_vs = read_vs
    self._ancestor_path = flat_path(ancestor)
    self._kv_iterator = RangeIterator(tr, tornado_fdb, desired_slice, limit,
                                      reverse, snapshot=True)
    self._done = False

  @gen.coroutine
  def next_page(self):
    if self._done:
      raise gen.Return(([], False))

    kvs, more_results = yield self._kv_iterator.next_page()
    usable_entries = []
    for kv in kvs:
      if self._read_vs is None and kv.value:
        self._kv_iterator.increase_limit()
        more_results = not self._kv_iterator.done_with_range
        continue

      if self.index.ancestor:
        entry = self.index.decode(kv, self._ancestor_path)
      else:
        entry = self.index.decode(kv)

      if self._read_vs is not None and entry.commit_vs > self._read_vs:
        self._kv_iterator.increase_limit()
        more_results = not self._kv_iterator.done_with_range
        continue

      if (self._read_vs is not None and entry.deleted_vs is not None and
          entry.deleted_vs < self._read_vs):
        self._kv_iterator.increase_limit()
        more_results = not self._kv_iterator.done_with_range
        continue

      usable_entries.append(entry)

    if not more_results:
      self._done = True

    raise gen.Return((usable_entries, more_results))


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
  def properties(self):
    return []

  def pack_method(self, versionstamp):
    if versionstamp.is_complete():
      return self.directory.pack
    else:
      return self.directory.pack_with_versionstamp

  def encode_path(self, path):
    raise NotImplementedError()

  def get_slice(self, filter_props):
    subspace = self.directory
    start = None
    stop = None
    for prop_name, filters in filter_props:
      if prop_name != KEY_PROP:
        raise BadRequest(u'Unexpected filter: {}'.format(prop_name))

      if len(filters) == 1 and filters[0][0] == Query_Filter.EQUAL:
        subspace = subspace.subspace(self.encode_path(filters[0][1]))
        continue

      for op, value in filters:
        if op in START_FILTERS:
          start = key_selector(op)(subspace.pack(self.encode_path(value)))
        elif op in STOP_FILTERS:
          stop = key_selector(op)(subspace.pack(self.encode_path(value)))
        else:
          raise BadRequest(u'Unexpected filter operation: {}'.format(op))

    start = start or subspace.range().start
    stop = stop or subspace.range().stop
    return slice(start, stop)


class KindlessIndex(Index):
  DIR_NAME = u'kindless'

  @classmethod
  def from_cache(cls, project_id, namespace, directory_cache):
    directory = directory_cache.get(
      (project_id, INDEX_DIR, namespace, cls.DIR_NAME))
    return cls(directory)

  def __repr__(self):
    dir_repr = u'/'.join([self.project_id, repr(self.namespace)])
    return u'KindlessIndex({})'.format(dir_repr)

  def encode_path(self, path):
    if isinstance(path, entity_pb.PropertyValue):
      path = flat_path(path.referencevalue())

    if isinstance(path, entity_pb.PropertyValue_ReferenceValue):
      path = flat_path(path)

    return path

  def encode(self, path, commit_vs=None):
    if commit_vs is None:
      commit_vs = fdb.tuple.Versionstamp()

    return self.pack_method(commit_vs)(path + (commit_vs,))

  def decode(self, kv):
    parts = self.directory.unpack(kv.key)
    deleted_vs = None
    if kv.value:
      deleted_vs = fdb.tuple.Versionstamp.from_bytes(kv.value)

    return IndexEntry(self.project_id, self.namespace, path=parts[:-1],
                      commit_vs=parts[-1], deleted_vs=deleted_vs)


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
    dir_repr = u'/'.join([self.project_id, repr(self.namespace), self.kind])
    return u'KindIndex({})'.format(dir_repr)

  def encode_path(self, path):
    if isinstance(path, entity_pb.PropertyValue):
      path = flat_path(path.referencevalue())

    if isinstance(path, entity_pb.PropertyValue_ReferenceValue):
      path = flat_path(path)

    kindless_path = path[:-2] + path[-1:]
    return kindless_path

  def encode(self, path, commit_vs=None):
    if commit_vs is None:
      commit_vs = fdb.tuple.Versionstamp()

    return self.pack_method(commit_vs)(self.encode_path(path) + (commit_vs,))

  def decode(self, kv):
    parts = self.directory.unpack(kv.key)
    kindless_path = parts[:-1]
    path = kindless_path[:-1] + (self.kind,) + kindless_path[-1:]
    deleted_vs = None
    if kv.value:
      deleted_vs = fdb.tuple.Versionstamp.from_bytes(kv.value)

    return IndexEntry(self.project_id, self.namespace, path=path,
                      commit_vs=parts[-1], deleted_vs=deleted_vs)


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

  def __repr__(self):
    dir_repr = u'/'.join([self.project_id, repr(self.namespace), self.kind,
                          self.prop_name])
    return u'SinglePropIndex({})'.format(dir_repr)

  def encode_path(self, path):
    if isinstance(path, entity_pb.PropertyValue):
      path = flat_path(path.referencevalue())

    if isinstance(path, entity_pb.PropertyValue_ReferenceValue):
      path = flat_path(path)

    kindless_path = path[:-2] + path[-1:]
    return kindless_path

  def encode(self, value, path, commit_vs=None):
    if commit_vs is None:
      commit_vs = fdb.tuple.Versionstamp()

    return self.pack_method(commit_vs)(
      encode_value(value) + self.encode_path(path) + (commit_vs,))

  def decode(self, kv):
    unpacked_key = self.directory.unpack(kv.key)
    value, remainder = pop_value(unpacked_key)
    kindless_path = remainder[:-1]
    path = kindless_path[:-1] + (self.kind,) + kindless_path[-1:]
    commit_vs = remainder[-1]
    deleted_vs = None
    if kv.value:
      deleted_vs = fdb.tuple.Versionstamp.from_bytes(kv.value)

    return PropertyEntry(self.project_id, self.namespace, path, self.prop_name,
                         value, commit_vs, deleted_vs)

  def get_slice(self, filter_props):
    subspace = self.directory
    start = None
    stop = None
    for prop_name, filters in filter_props:
      if prop_name == self.prop_name:
        encoder = encode_value
      elif prop_name == KEY_PROP:
        encoder = self.encode_path
      else:
        raise BadRequest(u'Unexpected filter: {}'.format(prop_name))

      if len(filters) == 1 and filters[0][0] == Query_Filter.EQUAL:
        subspace = subspace.subspace(encoder(filters[0][1]))
        continue

      for op, value in filters:
        if op in START_FILTERS:
          start = key_selector(op)(subspace.pack(encoder(value)))
        elif op in STOP_FILTERS:
          stop = key_selector(op)(subspace.pack(encoder(value)))
        else:
          raise BadRequest(u'Unexpected filter operation: {}'.format(op))

    start = start or subspace.range().start
    stop = stop or subspace.range().stop
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
    return self.directory.get_path()[6]

  @property
  def prop_names(self):
    return tuple(prop_name for prop_name, _ in self.order_info)

  @classmethod
  def from_cache(cls, project_id, namespace, index_id, kind, ancestor,
                 order_info, directory_cache):
    directory = directory_cache.get(
      (project_id, INDEX_DIR, namespace, cls.DIR_NAME, index_id))
    return cls(directory, kind, ancestor, order_info)

  def __repr__(self):
    components = [self.project_id, repr(self.namespace), self.kind]
    if self.ancestor:
      components.append(u'(includes ancestors)')

    for prop_name, direction in self.order_info:
      if direction == Query_Order.DESCENDING:
        prop_name = '-' + prop_name

      components.append(prop_name)

    return u'CompositeIndex({})'.format(u'/'.join(components))

  def encode_path(self, path):
    if isinstance(path, entity_pb.PropertyValue):
      path = flat_path(path.referencevalue())

    if isinstance(path, entity_pb.PropertyValue_ReferenceValue):
      path = flat_path(path)

    kindless_path = path[:-2] + path[-1:]
    return kindless_path

  def encode(self, prop_list, path, commit_vs=None):
    if commit_vs is None:
      commit_vs = fdb.tuple.Versionstamp()

    if self.ancestor and len(path) == 2:
      return []

    encoded_values_by_prop = []
    for index_prop_name in self.prop_names:
      encoded_values_by_prop.append(
        tuple(encode_value(prop.value()) for prop in prop_list
              if prop.name() == index_prop_name))

    pack = self.pack_method(commit_vs)
    encoded_values = itertools.product(*encoded_values_by_prop)
    if not self.ancestor:
      return tuple(pack(value + self.encode_path(path) + (commit_vs,))
                   for value in encoded_values)

    keys = []
    for index in range(2, len(path), 2):
      ancestor_path = path[:index]
      encoded_remaining_path = self.encode_path(path[index:])
      keys.extend(
        [pack(ancestor_path + value + encoded_remaining_path + (commit_vs,))
         for value in encoded_values])

    return tuple(keys)

  def decode(self, kv, ancestor_path=tuple()):
    remainder = self.directory.unpack(kv.key)[len(ancestor_path):]
    properties = []
    for prop_name in self.prop_names:
      value, remainder = pop_value(remainder)
      properties.append((prop_name, value))

    kindless = remainder[:-1]
    path = ancestor_path + kindless[:-1] + (self.kind,) + kindless[-1:]
    commit_vs = remainder[-1]
    deleted_vs = None
    if kv.value:
      deleted_vs = fdb.tuple.Versionstamp.from_bytes(kv.value)

    return CompositeEntry(self.project_id, self.namespace, path, properties,
                          commit_vs, deleted_vs)

  def get_slice(self, filter_props):
    subspace = self.directory
    start = None
    stop = None
    for prop_name, filters in filter_props:
      if prop_name == self.prop_name:
        encoder = self.encode_value
      elif prop_name == KEY_PROP:
        encoder = self.encode_path
      else:
        raise BadRequest(u'Unexpected filter: {}'.format(prop_name))

      if len(filters) == 1 and filters[0][0] == Query_Filter.EQUAL:
        subspace = subspace.subspace(encoder(filters[0][1]))
        continue

      for op, value in filters:
        if op in START_FILTERS:
          start = key_selector(op)(subspace.pack(encoder(value)))
        elif op in STOP_FILTERS:
          stop = key_selector(op)(subspace.pack(encoder(value)))
        else:
          raise BadRequest(u'Unexpected filter operation: {}'.format(op))

    start = start or subspace.range().start
    stop = stop or subspace.range().stop
    return slice(start, stop)


class IndexManager(object):
  _MAX_RESULTS = 300

  def __init__(self, db, directory_cache, tornado_fdb):
    self.composite_index_manager = None
    self._db = db
    self._directory_cache = directory_cache
    self._tornado_fdb = tornado_fdb

  def put_entries(self, tr, old_entity, old_vs, new_entity):
    if old_entity is not None:
      for key in self._get_index_keys(old_entity, old_vs):
        tr.set_versionstamped_value(key, b'\x00' * 14)

    if new_entity is not None:
      for key in self._get_index_keys(new_entity):
        tr.set_versionstamped_key(key, b'')

  def get_reverse(self, query):
    if not query.order_list():
      return False

    if query.order_size() > 1:
      raise BadRequest(u'Only one order can be specified')

    if query.order(0).property() != query.filter(-1).property().name():
      raise BadRequest(u'Only the last filter property can be ordered')

    return query.order(0).direction() == query.order(0).DESCENDING

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
    index = self._get_perfect_index(query)
    reverse = self.get_reverse(query)
    desired_slice = index.get_slice(filter_props)
    rpc_limit, check_more_results = self.rpc_limit(query)
    fetch_limit = rpc_limit
    if check_more_results:
      fetch_limit += 1

    iterator = IndexIterator(tr, self._tornado_fdb, desired_slice, fetch_limit,
                             reverse, index, read_vs)
    logger.debug('using index: {}'.format(index))
    raise BadRequest(u'Query is not supported')

    return iterator

  @gen.coroutine
  def update_composite_index(self, project_id, index_pb, cursor=None):
    start_ns = ''
    start_key = ''
    if cursor is not None:
      start_ns, start_key = cursor

    kind = six.text_type(index_pb.definition().entity_type())
    ancestor = index_pb.definition().ancestor()
    order_info = ((prop.name(), prop.direction())
                  for prop in index_pb.definition().property_list())
    indexes_dir = self._directory_cache.get((project_id, INDEX_DIR))
    # , namespace, cls.DIR_NAME, index_id))
    tr = self._db.create_transaction()
    deadline = time.time() + MAX_FDB_TX_DURATION / 2
    for namespace in indexes_dir.list(tr):
      if namespace < start_ns:
        continue

      kind_index_dir = indexes_dir.open(
        tr, (namespace, KindIndex.DIR_NAME, kind))
      kind_index = KindIndex(kind_index_dir)
      iterator = IndexIterator()
      index_dir = index_dir.open(tr, (namespace, CompositeIndex.DIR_NAME,
                                      index_pb.id()))
      index = CompositeIndex(index_dir, kind, ancestor, order_info)


    if time.time() > deadline:
      try:
        yield self._tornado_fdb.commit(tr)
      except fdb.FDBError as fdb_error:
        tr.on_error(fdb_error).wait()

      tr = self._db.create_transaction()

    index = CompositeIndex.from_cache(project_id, )
    logger.info(u'Updating index: {}'.format(index))
    entries_updated = 0

    # TODO: Adjust prefix based on ancestor.
    prefix = '{app}{delimiter}{entity_type}{kind_separator}'.format(
      app=app_id,
      delimiter=self._SEPARATOR * 2,
      entity_type=entity_type,
      kind_separator=dbconstants.KIND_SEPARATOR,
    )
    start_row = prefix
    end_row = prefix + self._TERM_STRING
    start_inclusive = True

    while True:
      # Fetch references from the kind table since entity keys can have a
      # parent prefix.
      references = yield self.datastore_batch.range_query(
        table_name=dbconstants.APP_KIND_TABLE,
        column_names=dbconstants.APP_KIND_SCHEMA,
        start_key=start_row,
        end_key=end_row,
        limit=self.BATCH_SIZE,
        offset=0,
        start_inclusive=start_inclusive,
      )

      pb_entities = yield self.__fetch_entities(references)
      entities = [entity_pb.EntityProto(entity) for entity in pb_entities]

      yield self.insert_composite_indexes(entities, [index])
      entries_updated += len(entities)

      # If we fetched fewer references than we asked for, we're done.
      if len(references) < self.BATCH_SIZE:
        break

      start_row = references[-1].keys()[0]
      start_inclusive = self._DISABLE_INCLUSIVITY

    logger.info('Updated {} index entries.'.format(entries_updated))

  def _get_index_keys(self, entity, commit_vs=None):
    project_id = six.text_type(entity.key().app())
    namespace = six.text_type(entity.key().name_space())
    path = flat_path(entity.key())
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
      prop_name = six.text_type(prop.name())
      entity_prop_names.append(prop_name)
      index = SinglePropIndex.from_cache(
        project_id, namespace, kind, prop_name, self._directory_cache)
      all_keys.append(index.encode(prop.value(), path, commit_vs))

    for index in composite_indexes:
      if not all(index_prop_name in entity_prop_names
                 for index_prop_name in index.prop_names):
        continue

      all_keys.extend([index.encode(entity.property_list(), path, commit_vs)])

    return all_keys

  def _get_perfect_index(self, query):
    project_id = six.text_type(query.app())
    namespace = six.text_type(query.name_space())
    filter_info = group_filters(query)
    order_info = get_order_info(query)
    prop_names = [prop_name for prop_name, _ in order_info]

    if not query.has_kind():
      if not all(prop_name == KEY_PROP for prop_name in prop_names):
        raise BadRequest(u'kind must be specified when filtering or ordering '
                         u'properties other than __key__')

      return KindlessIndex.from_cache(
        project_id, namespace, self._directory_cache)

    kind = six.text_type(query.kind())
    if all(prop_name == KEY_PROP for prop_name in prop_names):
      return KindIndex.from_cache(
        project_id, namespace, kind, self._directory_cache)

    if sum(prop_name != KEY_PROP for prop_name in prop_names) == 1:
      inequality_filters = [filters for _, filters in filter_info
                            if filters[0][0] != Query_Filter.EQUAL]
      if not query.has_ancestor() or not inequality_filters:
        prop_name = next(prop_name != KEY_PROP for prop_name in prop_names)
        return SinglePropIndex.from_cache(
          project_id, namespace, six.text_type(query.kind()), prop_name,
          self._directory_cache)

    index_to_use = _FindIndexToUse(query, self._get_indexes_pb(project_id))
    if index_to_use is not None:
      # result = yield self.composite_v2(query, filter_info, index_to_use)
      index_to_use.
      index = CompositeIndex.from_cache()

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
