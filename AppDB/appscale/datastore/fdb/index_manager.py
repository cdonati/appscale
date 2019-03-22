import logging
import sys

from tornado import gen

from appscale.common.unpackaged import APPSCALE_PYTHON_APPSERVER
from appscale.datastore.fdb.utils import (
  decode_path, fdb, flat_path, RangeIterator)
from appscale.datastore.dbconstants import BadRequest, InternalError

sys.path.append(APPSCALE_PYTHON_APPSERVER)
from google.appengine.datastore import datastore_pb, entity_pb

logger = logging.getLogger(__name__)

INDEX_DIR = 'indexes'


class ValueTypes(object):
  INT64 = '1'
  BOOLEAN = '2'
  STRING = '3'
  DOUBLE = '4'
  # POINT = '5'
  # USER = '6'
  REFERENCE = '7'


SCALAR_TYPES = (ValueTypes.INT64, ValueTypes.BOOLEAN, ValueTypes.STRING,
                ValueTypes.DOUBLE)


def get_type(value):
  valid_types = [name.lower() for name, value in ValueTypes.__dict__.items()
                 if not name.startswith('_')]
  for name, encoded_value in valid_types:
    if getattr(value, 'has_{}value'.format(name))():
      return name, encoded_value

  raise BadRequest('Unknown PropertyValue type')


def get_type_name(encoded_type):
  return next(key for key, val in ValueTypes.__dict__.items()
              if val == encoded_type).lower()


def group_filters(query):
  filter_props = []
  for query_filter in query.filter_list():
    if query_filter.property_size() != 1:
      raise BadRequest('Each filter must have exactly one property')

    prop = query_filter.property(0)
    filter_info = (query_filter.op(), prop.value())
    if filter_props and prop.name() == filter_props[-1][0]:
      filter_props[-1][1].append(filter_info)
    else:
      filter_props.append((prop.name(), [filter_info]))

  for name, filters in filter_props[:-1]:
    if name == '__key__':
      raise BadRequest('Only the last filter property can be on __key__')

    if len(filters) != 1 or filters[0][0] != datastore_pb.Query_Filter.EQUAL:
      raise BadRequest('All but the last property must be equality filters')

  if len(filter_props[-1][1]) > 2:
    raise BadRequest('A property can only have up to two filters')

  return filter_props


class IndexEntry(object):
  __SLOTS__ = ['project_id', 'namespace', 'path', 'commit_vs']

  def __init__(self, project_id, namespace, path, commit_vs):
    self.project_id = project_id
    self.namespace = namespace
    self.path = path
    self.commit_vs = commit_vs

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


class KeyEntry(IndexEntry):
  def __init__(self, project_id, namespace, path, commit_vs):
    super(KeyEntry, self).__init__(project_id, namespace, path, commit_vs)

  def result(self):
    entity = entity_pb.EntityProto()
    entity.mutable_key().MergeFrom(self.key)
    entity.mutable_entity_group().MergeFrom(self.group)
    return entity


class PropertyEntry(IndexEntry):
  __SLOTS__ = ['prop_name', 'value']

  def __init__(self, project_id, namespace, path, prop_name, value, commit_vs):
    super(PropertyEntry, self).__init__(project_id, namespace, path, commit_vs)
    self.prop_name = prop_name
    self.value = value

  def result(self):
    entity = entity_pb.EntityProto()
    entity.mutable_key().MergeFrom(self.key)
    entity.mutable_entity_group().MergeFrom(self.group)
    prop = entity.add_property()
    prop.set_name(self.prop_name)
    prop.set_meaning(entity_pb.Property.INDEX_VALUE)
    prop.mutable_value().MergeFrom(self.value)
    return entity


class IndexIterator(object):
  def __init__(self, index, kv_iterator):
    self._kv_iterator = kv_iterator
    self._index = index

  @gen.coroutine
  def next_page(self):
    kvs, more_results = yield self._kv_iterator.next_page()
    raise gen.Return(([self._index.decode(kv) for kv in kvs], more_results))


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

  def pack_method(self, versionstamp):
    if versionstamp.is_complete():
      return self.directory.pack
    else:
      return self.directory.pack_with_versionstamp


class KindlessIndex(Index):
  DIR_NAME = 'kindless'

  def __init__(self, project_id, namespace, directory_cache):
    directory = directory_cache.get(
      (project_id, INDEX_DIR, namespace, self.DIR_NAME))
    super(KindlessIndex, self).__init__(directory)

  def __repr__(self):
    dir_repr = '/'.join([self.project_id, repr(self.namespace)])
    return 'KindlessIndex({})'.format(dir_repr)

  def encode(self, path, include_vs=True, commit_vs=fdb.tuple.Versionstamp()):
    if not include_vs:
      return self.directory.pack((path,))

    return self.pack_method(commit_vs)(path + (commit_vs,))

  def decode(self, kv):
    parts = self.directory.unpack(kv.key)
    return KeyEntry(self.project_id, self.namespace, path=parts[:-1],
                    commit_vs=parts[-1])


class KindIndex(Index):
  DIR_NAME = 'kind'

  def __init__(self, project_id, namespace, kind, directory_cache):
    directory = directory_cache.get(
      (project_id, INDEX_DIR, namespace, self.DIR_NAME, kind))
    super(KindIndex, self).__init__(directory)

  @property
  def kind(self):
    return self.directory.get_path()[-1]

  def __repr__(self):
    dir_repr = '/'.join([self.project_id, repr(self.namespace), self.kind])
    return 'KindIndex({})'.format(dir_repr)

  def encode(self, path, include_vs=True, commit_vs=fdb.tuple.Versionstamp()):
    kindless_path = path[:-2] + path[-1:]
    if not include_vs:
      return self.directory.pack((kindless_path,))

    return self.pack_method(commit_vs)(kindless_path + (commit_vs,))

  def decode(self, kv):
    parts = self.directory.unpack(kv.key)
    kindless_path = parts[:-1]
    path = kindless_path[:-1] + (self.kind,) + kindless_path[-1:]
    return KeyEntry(self.project_id, self.namespace, path=path,
                    commit_vs=parts[-1])


class SinglePropIndex(Index):
  DIR_NAME = 'single-property'

  # Allows the decoder to differentiate between the property value and the path
  # when the value has a variable number of elements.
  _DELIMITER = '\x00'

  def __init__(self, project_id, namespace, kind, prop_name, directory_cache):
    directory = directory_cache.get(
      (project_id, INDEX_DIR, namespace, self.DIR_NAME, kind, prop_name))
    super(SinglePropIndex, self).__init__(directory)

  @property
  def kind(self):
    return self.directory.get_path()[-2]

  @property
  def prop_name(self):
    return self.directory.get_path()[-1]

  def encode_value(self, value):
    type_name, encoded_type = get_type(value)
    if encoded_type in SCALAR_TYPES:
      encoded_value = getattr(value, '{}value'.format(type_name))()
      return (encoded_type, encoded_value)
    elif encoded_type == ValueTypes.REFERENCE:
      encoded_value = (value.app(), value.name_space()) + flat_path(value)
      return (encoded_type,) + encoded_value + (self._DELIMITER,)
    else:
      raise BadRequest('Unknown PropertyValue type')

  def encode(self, value, path, commit_vs=fdb.tuple.Versionstamp(),
             omit_vs=False):
    encoded_value = self.encode_value(value)
    kindless_path = path[:-2] + path[-1:]
    if omit_vs:
      return self.directory.pack(encoded_value + kindless_path)

    return self.pack_method(commit_vs)(encoded_value + kindless_path +
                                       (commit_vs,))

  def apply_filters(self, filter_props):
    subspace = self.directory
    start = None
    end = None
    for prop_name, filters in filter_props:
      if prop_name == self.prop_name:
        for op, value in filters:
          encoded_value = self.encode_value(value)
          if op == datastore_pb.Query_Filter.EQUAL:
            subspace = self.directory.subspace(encoded_value)
          elif op == datastore_pb.Query_Filter.LESS_THAN:
            end = fdb.KeySelector.last_less_than, encoded_value
          elif op == datastore_pb.Query_Filter.LESS_THAN_OR_EQUAL:
            end = fdb.KeySelector.last_less_or_equal, encoded_value
          elif op == datastore_pb.Query_Filter.GREATER_THAN:
            start = fdb.KeySelector.first_greater_than, encoded_value
          elif op == datastore_pb.Query_Filter.GREATER_THAN_OR_EQUAL:
            start = fdb.KeySelector.first_greater_than, encoded_value
          else:
            raise BadRequest('Unrecognized filter operation')

      for op, value in filters:
        if op == datastore_pb.Query_Filter.EQUAL:
          subspace =

  def decode(self, kv):
    parts = self.directory.unpack(kv.key)
    encoded_type = parts[0]
    type_name = get_type_name(encoded_type)
    value = entity_pb.PropertyValue()
    if encoded_type == ValueTypes.REFERENCE:
      delimiter_index = parts.index(self._DELIMITER)
      value_parts = parts[1:delimiter_index]
      reference_val = value.mutable_referencevalue()
      reference_val.set_app(value_parts[0])
      reference_val.set_name_space(value_parts[1])
      reference_val.MergeFrom(
        decode_path(value_parts[2:], reference_value=True))
      kindless_path = parts[slice(delimiter_index + 1, -1)]
    elif encoded_type in SCALAR_TYPES:
      getattr(value, 'set_{}value'.format(type_name))(parts[1])
      kindless_path = parts[1:-1]
    else:
      raise InternalError('Unknown PropertyValue type')

    path = kindless_path[:-1] + (self.kind,) + kindless_path[-1:]
    return PropertyEntry(self.project_id, self.namespace, path, self.prop_name,
                         value, commit_vs=parts[-1])

  def __repr__(self):
    dir_repr = '/'.join([self.project_id, repr(self.namespace), self.kind,
                         self.prop_name])
    return 'SinglePropIndex({})'.format(dir_repr)


class IndexManager(object):
  _INDEX_DIR = 'indexes'

  _MAX_RESULTS = 300

  def __init__(self, directory_cache, tornado_fdb):
    self._directory_cache = directory_cache
    self._tornado_fdb = tornado_fdb

  def put_entries(self, tr, old_entity, old_vs, new_entity):
    project_id = new_entity.key().app()
    namespace = new_entity.key().name_space()
    path = flat_path(new_entity.key())
    kind = path[-2]

    kindless_index = KindlessIndex(project_id, namespace,
                                   self._directory_cache)
    kind_index = KindIndex(project_id, namespace, kind, self._directory_cache)

    if old_entity is not None:
      del tr[kindless_index.encode(path, old_vs)]
      del tr[kind_index.encode(path, old_vs)]
      for prop in old_entity.property_list():
        index = SinglePropIndex(project_id, namespace, kind, prop.name(),
                                self._directory_cache)
        del tr[index.encode(prop.value(), path, old_vs)]

    if new_entity is not None:
      tr.set_versionstamped_key(kindless_index.encode(path), '')
      tr.set_versionstamped_key(kind_index.encode(path), '')
      for prop in new_entity.property_list():
        index = SinglePropIndex(project_id, namespace, kind, prop.name(),
                                self._directory_cache)
        tr.set_versionstamped_key(index.encode(prop.value(), path), '')

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

  def fetch_data(self, query):
    if query.keys_only():
      return False

    if not query.property_name_list():
      return True

    if query.property_name(0) != '__key__' or query.property_name_size() > 1:
      raise BadRequest('Invalid property name list')

    return False

  def get_iterator(self, tr, query):
    filter_props = group_filters(query)
    if not query.has_kind():
      index = KindlessIndex(query.app(), query.name_space(),
                            self._directory_cache)
    elif all([name == '__key__' for name, _ in filter_props]):
      index = KindIndex(query.app(), query.name_space(), query.kind(),
                        self._directory_cache)
    elif sum([name != '__key__' for name, _ in filter_props]) == 1:
      prop_name, filters = filter_props[0]
      index = SinglePropIndex(query.app(), query.name_space(), query.kind(),
                              prop_name, self._directory_cache)
    else:
      raise BadRequest('Query is not supported')

    reverse = self.get_reverse(query)
    subspace = index.directory
    for prop_name, filters in filter_props:
      for op, value in filters:
        if op == datastore_pb.Query_Filter.EQUAL:
          subspace =
      subspace = index.apply_filter
    for prop_filter in query.filter_list():
      if prop_filter.property_size() != 1:
        raise BadRequest('Invalid filter list')

      value = prop_filter.property(0).value()

    if isinstance(index, KeyIndex):
      range_ = index.directory.range()
      for filter_ in query.filter_list():
        if filter_.property_size() != 1 or filter_.property(0) != '__key__':
          raise BadRequest('Invalid filter list')

        path = flat_path(filter_.property(0).value().referencevalue())


    rpc_limit, check_more_results = self.rpc_limit(query)
    fetch_limit = rpc_limit
    if check_more_results:
      fetch_limit += 1

    kv_iterator = RangeIterator(tr, self._tornado_fdb, index.directory.range(),
                                fetch_limit, reverse, snapshot=True)
    iterator = IndexIterator(index, kv_iterator)
    logger.debug('using index: {}'.format(index))
    return iterator
