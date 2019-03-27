"""
indexes: This contains a directory for each index that the datastore needs in
order to satisfy basic queries along with indexes that the project has defined
for composite queries. Here is an example template:

  ([index dir^4], <type>, <value>, [path], <commit versionstamp>) -> ''

^4: The index's directory path. For example,
    (appscale, datastore, <project>, indexes, <namespace>, single-property,
     <kind>, <property name>)
"""

import logging
import sys

import six
from tornado import gen

from appscale.common.unpackaged import APPSCALE_PYTHON_APPSERVER
from appscale.datastore.fdb.utils import (
  decode_path, fdb, flat_path, RangeIterator)
from appscale.datastore.dbconstants import BadRequest, InternalError

sys.path.append(APPSCALE_PYTHON_APPSERVER)
from google.appengine.datastore import datastore_pb, entity_pb
from google.appengine.datastore.datastore_pb import Query_Filter

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


START_FILTERS = (Query_Filter.GREATER_THAN_OR_EQUAL, Query_Filter.GREATER_THAN)
STOP_FILTERS = (Query_Filter.LESS_THAN_OR_EQUAL, Query_Filter.LESS_THAN)


def get_type(value):
  readable_types = [
    (name.lower(), encoded) for name, encoded in V3Types.__dict__.items()
    if not name.startswith('_') and name != 'NULL']
  for type_name, encoded_type in readable_types:
    if getattr(value, 'has_{}value'.format(type_name))():
      return type_name, encoded_type

  return None, V3Types.NULL


def get_type_name(encoded_type):
  return next(key for key, val in V3Types.__dict__.items()
              if val == encoded_type).lower()


def group_filters(query):
  filter_props = []
  for query_filter in query.filter_list():
    if query_filter.property_size() != 1:
      raise BadRequest('Each filter must have exactly one property')

    prop = query_filter.property(0)
    prop_name = six.text_type(prop.name())
    filter_info = (query_filter.op(), prop.value())
    if filter_props and prop_name == filter_props[-1][0]:
      filter_props[-1][1].append(filter_info)
    else:
      filter_props.append((prop_name, [filter_info]))

  for name, filters in filter_props[:-1]:
    if name == KEY_PROP:
      raise BadRequest('Only the last filter property can be on __key__')

    if len(filters) != 1 or filters[0][0] != datastore_pb.Query_Filter.EQUAL:
      raise BadRequest('All but the last property must be equality filters')

  if len(filter_props[-1][1]) > 2:
    raise BadRequest('A property can only have up to two filters')

  return filter_props


def key_selector(op):
  """ Like Python's slice notation, FDB range queries include the start and
      exclude the stop. Therefore, the stop selector must point to the first
      key that will be excluded from the results. """
  if op in (Query_Filter.GREATER_THAN_OR_EQUAL, Query_Filter.LESS_THAN):
    return fdb.KeySelector.first_greater_or_equal
  elif op in (Query_Filter.GREATER_THAN, Query_Filter.LESS_THAN_OR_EQUAL):
    return fdb.KeySelector.first_greater_than
  else:
    raise BadRequest('Unsupported filter operator')


class IndexEntry(object):
  __SLOTS__ = ['project_id', 'namespace', 'path', 'commit_vs']

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
  __SLOTS__ = ['prop_name', 'value']

  def __init__(self, project_id, namespace, path, prop_name, value, commit_vs,
               deleted_vs):
    super(PropertyEntry, self).__init__(project_id, namespace, path, commit_vs,
                                        deleted_vs)
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


class IndexIterator(object):
  def __init__(self, tr, tornado_fdb, desired_slice, limit, reverse, index,
               read_vs=None):
    self._kv_iterator = RangeIterator(tr, tornado_fdb, desired_slice, limit,
                                      reverse, snapshot=True)
    self.index = index
    self._read_vs = read_vs

  @gen.coroutine
  def next_page(self):
    kvs, more_results = yield self._kv_iterator.next_page()
    usable_entries = []
    for kv in kvs:
      if self._read_vs is None and kv.value:
        self._kv_iterator.increase_limit()
        more_results = not self._kv_iterator.done_with_range
        continue

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

    raise gen.Return((usable_entries, more_results))


class Index(object):
  __SLOTS__ = ['directory']

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
        raise BadRequest('Unexpected filter: {}'.format(prop_name))

      if len(filters) == 1 and filters[0][0] == Query_Filter.EQUAL:
        subspace = subspace.subspace(self.encode_path(filters[0][1]))
        continue

      for op, value in filters:
        if op in START_FILTERS:
          start = key_selector(op)(subspace.pack(self.encode_path(value)))
        elif op in STOP_FILTERS:
          stop = key_selector(op)(subspace.pack(self.encode_path(value)))
        else:
          raise BadRequest('Unexpected filter operation: {}'.format(op))

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
    dir_repr = '/'.join([self.project_id, repr(self.namespace)])
    return u'KindlessIndex({})'.format(dir_repr)

  def encode_path(self, path):
    if isinstance(path, entity_pb.PropertyValue):
      path = flat_path(path.referencevalue())

    if isinstance(path, entity_pb.PropertyValue_ReferenceValue):
      path = flat_path(path)

    return path

  def encode(self, path, commit_vs=fdb.tuple.Versionstamp()):
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
    dir_repr = '/'.join([self.project_id, repr(self.namespace), self.kind])
    return u'KindIndex({})'.format(dir_repr)

  def encode_path(self, path):
    if isinstance(path, entity_pb.PropertyValue):
      path = flat_path(path.referencevalue())

    if isinstance(path, entity_pb.PropertyValue_ReferenceValue):
      path = flat_path(path)

    kindless_path = path[:-2] + path[-1:]
    return kindless_path

  def encode(self, path, commit_vs=fdb.tuple.Versionstamp()):
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

  # Allows the decoder to differentiate between the property value and the path
  # when the value has a variable number of elements.
  _DELIMITER = '\x00'

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
  def properties(self):
    return [self.prop_name]

  def __repr__(self):
    dir_repr = '/'.join([self.project_id, repr(self.namespace), self.kind,
                         self.prop_name])
    return u'SinglePropIndex({})'.format(dir_repr)

  def encode_value(self, value):
    type_name, encoded_type = get_type(value)
    if encoded_type == V3Types.NULL:
      return (encoded_type,)

    if encoded_type not in COMPOUND_TYPES:
      encoded_value = getattr(value, '{}value'.format(type_name))()
      if encoded_type == V3Types.STRING:
        encoded_value = six.text_type(encoded_value)

      return encoded_type, encoded_value

    if encoded_type == V3Types.POINT:
      return encoded_type, (value.x(), value.y())

    if encoded_type == V3Types.USER:
      return encoded_type, (six.text_type(value.email()),
                            six.text_type(value.auth_domain()))

    if encoded_type == V3Types.REFERENCE:
      project_id = six.text_type(value.app())
      namespace = six.text_type(value.name_space())
      encoded_value = (project_id, namespace) + flat_path(value)
      return (encoded_type,) + encoded_value + (self._DELIMITER,)

    raise BadRequest('{} is not a supported value'.format(type_name))

  def encode_path(self, path):
    if isinstance(path, entity_pb.PropertyValue):
      path = flat_path(path.referencevalue())

    if isinstance(path, entity_pb.PropertyValue_ReferenceValue):
      path = flat_path(path)

    kindless_path = path[:-2] + path[-1:]
    return kindless_path

  def encode(self, value, path, commit_vs=fdb.tuple.Versionstamp()):
    return self.pack_method(commit_vs)(
      self.encode_value(value) + self.encode_path(path) + (commit_vs,))

  def pop_value(self, unpacked_key):
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
      delimiter_index = unpacked_key.index(self._DELIMITER)
      value_parts = unpacked_key[1:delimiter_index]
      reference_val = value.mutable_referencevalue()
      reference_val.set_app(value_parts[0])
      reference_val.set_name_space(value_parts[1])
      reference_val.MergeFrom(
        decode_path(value_parts[2:], reference_value=True))
      return value, unpacked_key[slice(delimiter_index + 1, None)]

    raise InternalError('Unsupported PropertyValue type')

  def decode(self, kv):
    unpacked_key = self.directory.unpack(kv.key)
    value, remainder = self.pop_value(unpacked_key)
    kindless_path = remainder[:-1]
    path = kindless_path[:-1] + (self.kind,) + kindless_path[-1:]
    deleted_vs = None
    if kv.value:
      deleted_vs = fdb.tuple.Versionstamp.from_bytes(kv.value)

    return PropertyEntry(self.project_id, self.namespace, path, self.prop_name,
                         value, commit_vs=remainder[-1], deleted_vs=deleted_vs)

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
        raise BadRequest('Unexpected filter: {}'.format(prop_name))

      if len(filters) == 1 and filters[0][0] == Query_Filter.EQUAL:
        subspace = subspace.subspace(encoder(filters[0][1]))
        continue

      for op, value in filters:
        if op in START_FILTERS:
          start = key_selector(op)(subspace.pack(encoder(value)))
        elif op in STOP_FILTERS:
          stop = key_selector(op)(subspace.pack(encoder(value)))
        else:
          raise BadRequest('Unexpected filter operation: {}'.format(op))

    start = start or subspace.range().start
    stop = stop or subspace.range().stop
    return slice(start, stop)


class IndexManager(object):
  _MAX_RESULTS = 300

  def __init__(self, directory_cache, tornado_fdb):
    self._directory_cache = directory_cache
    self._tornado_fdb = tornado_fdb

  def put_entries(self, tr, old_entity, old_vs, new_entity):
    project_id = six.text_type(new_entity.key().app())
    namespace = six.text_type(new_entity.key().name_space())
    path = flat_path(new_entity.key())
    kind = path[-2]

    kindless_index = KindlessIndex.from_cache(
      project_id, namespace, self._directory_cache)
    kind_index = KindIndex.from_cache(
      project_id, namespace, kind, self._directory_cache)

    if old_entity is not None:
      tr.set_versionstamped_value(kindless_index.encode(path, old_vs),
                                  b'\x00' * 14)
      tr.set_versionstamped_value(kind_index.encode(path, old_vs),
                                  b'\x00' * 14)
      for prop in old_entity.property_list():
        prop_name = six.text_type(prop.name())
        index = SinglePropIndex.from_cache(
          project_id, namespace, kind, prop_name, self._directory_cache)
        tr.set_versionstamped_value(index.encode(prop.value(), path, old_vs),
                                    b'\x00' * 14)

    if new_entity is not None:
      tr.set_versionstamped_key(kindless_index.encode(path), b'')
      tr.set_versionstamped_key(kind_index.encode(path), b'')
      for prop in new_entity.property_list():
        prop_name = six.text_type(prop.name())
        index = SinglePropIndex.from_cache(
          project_id, namespace, kind, prop_name, self._directory_cache)
        tr.set_versionstamped_key(index.encode(prop.value(), path), b'')

  def get_reverse(self, query):
    if not query.order_list():
      return False

    if query.order_size() > 1:
      raise BadRequest('Only one order can be specified')

    if query.order(0).property() != query.filter(-1).property().name():
      raise BadRequest('Only the last filter property can be ordered')

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
      raise BadRequest('A keys-only query cannot include a property name list')

    if query.keys_only():
      return False

    if not query.property_name_list():
      return True

    return False

  def get_iterator(self, tr, query, read_vs=None):
    project_id = six.text_type(query.app())
    namespace = six.text_type(query.name_space())
    filter_props = group_filters(query)
    if not query.has_kind():
      index = KindlessIndex.from_cache(
        project_id, namespace, self._directory_cache)
    elif all([name == KEY_PROP for name, _ in filter_props]):
      index = KindIndex.from_cache(
        project_id, namespace, six.text_type(query.kind()),
        self._directory_cache)
    elif sum([name != KEY_PROP for name, _ in filter_props]) == 1:
      prop_name, filters = filter_props[0]
      index = SinglePropIndex.from_cache(
        project_id, namespace, six.text_type(query.kind()), prop_name,
        self._directory_cache)
    else:
      index_to_use = _FindIndexToUse(query, self.get_indexes(app_id))
      if index_to_use is not None:
        result = yield self.composite_v2(query, filter_info, index_to_use)
        raise gen.Return(result)

      raise BadRequest('Query is not supported')

    reverse = self.get_reverse(query)
    desired_slice = index.get_slice(filter_props)
    rpc_limit, check_more_results = self.rpc_limit(query)
    fetch_limit = rpc_limit
    if check_more_results:
      fetch_limit += 1

    iterator = IndexIterator(tr, self._tornado_fdb, desired_slice, fetch_limit,
                             reverse, index, read_vs)
    logger.debug('using index: {}'.format(index))
    return iterator
