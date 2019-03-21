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


class PropertyTypes(object):
  INT_64 = 'int64'
  BOOLEAN = 'boolean'
  STRING = 'string'
  DOUBLE = 'double'
  REFERENCE = 'reference'


SCALAR_TYPES = (PropertyTypes.INT_64, PropertyTypes.BOOLEAN,
                PropertyTypes.STRING, PropertyTypes.DOUBLE)


def prop_type(value):
  valid_types = [val for key, val in PropertyTypes.__dict__.items()
                 if not key.startswith('_')]
  for valid_type in valid_types:
    if getattr(value, 'has_{}value'.format(valid_type))():
      return valid_type

  raise BadRequest('Unknown PropertyValue type')


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


class KeyIndex(Index):
  def encode(self, path, include_vs=True, commit_vs=fdb.tuple.Versionstamp()):
    if not include_vs:
      return self.directory.pack((path,))

    return self.pack_method(commit_vs)(path + (commit_vs,))

  def decode(self, kv):
    parts = self.directory.unpack(kv.key)
    return KeyEntry(self.project_id, self.namespace, path=parts[:-1],
                    commit_vs=parts[-1])


class KindlessIndex(KeyIndex):
  DIR_NAME = 'kindless'

  def __init__(self, project_id, namespace, directory_cache):
    directory = directory_cache.get(
      (project_id, INDEX_DIR, namespace, self.DIR_NAME))
    super(KindlessIndex, self).__init__(directory)

  def __repr__(self):
    path = '/'.join([self.project_id, repr(self.namespace)])
    return 'KindlessIndex({})'.format(path)


class KindIndex(KeyIndex):
  DIR_NAME = 'kind'

  def __init__(self, project_id, namespace, kind, directory_cache):
    directory = directory_cache.get(
      (project_id, INDEX_DIR, namespace, self.DIR_NAME, kind))
    super(KindIndex, self).__init__(directory)

  @property
  def kind(self):
    return self.directory.get_path()[-1]

  def __repr__(self):
    path = '/'.join([self.project_id, repr(self.namespace), self.kind])
    return 'KindIndex({})'.format(path)


class SinglePropIndex(Index):
  DIR_NAME = 'single-property'

  # Allows the decoder to differentiate between the property value and the path
  # when the value has more than one element.
  _DELIMITER = '\x00'

  def __init__(self, project_id, namespace, kind, prop_name, value,
               directory_cache):
    directory = directory_cache.get(
      (project_id, INDEX_DIR, namespace, self.DIR_NAME, kind, prop_name,
       prop_type(value)))
    super(SinglePropIndex, self).__init__(directory)

  @property
  def kind(self):
    return self.directory.get_path()[-3]

  @property
  def prop_name(self):
    return self.directory.get_path()[-2]

  @property
  def type(self):
    return self.directory.get_path()[-1]

  def encode(self, value, path, include_vs=True,
             commit_vs=fdb.tuple.Versionstamp()):
    if self.type in SCALAR_TYPES:
      encoded_value = getattr(value, '{}value'.format(self.type))()
      prefix = (encoded_value,)
    elif self.type == PropertyTypes.REFERENCE:
      encoded_value = (value.app(), value.name_space()) + flat_path(value)
      prefix = encoded_value + (self._DELIMITER,)
    else:
      raise BadRequest('Unknown PropertyValue type')

    if not include_vs:
      return self.directory.pack(prefix + path)

    return self.pack_method(commit_vs)(prefix + path + (commit_vs,))

  def decode(self, kv):
    parts = self.directory.unpack(kv.key)
    value = entity_pb.PropertyValue()
    if self.type == PropertyTypes.REFERENCE:
      delimiter_index = parts.index(self._DELIMITER)
      value_parts = parts[:delimiter_index]
      reference_val = value.mutable_referencevalue()
      reference_val.set_app(value_parts[0])
      reference_val.set_name_space(value_parts[1])
      reference_val.MergeFrom(
        decode_path(value_parts[2:], reference_value=True))
      path = parts[slice(delimiter_index + 1, -1)]
    elif self.type in SCALAR_TYPES:
      getattr(value, 'set_{}value'.format(self.type))(parts[0])
      path = parts[1:-1]
    else:
      raise InternalError('Unknown PropertyValue type')

    return PropertyEntry(self.project_id, self.namespace, path, self.prop_name,
                         value, commit_vs=parts[-1])

  def __repr__(self):
    path = '/'.join([self.project_id, repr(self.namespace), self.kind,
                     self.prop_name, self.type])
    return 'SinglePropIndex({})'.format(path)


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
                                prop.value(), self._directory_cache)
        del tr[index.encode(prop.value(), path, old_vs)]

    if new_entity is not None:
      tr.set_versionstamped_key(kindless_index.encode(path), '')
      tr.set_versionstamped_key(kind_index.encode(path), '')
      for prop in new_entity.property_list():
        index = SinglePropIndex(project_id, namespace, kind, prop.name(),
                                prop.value(), self._directory_cache)
        tr.set_versionstamped_key(index.encode(prop.value(), path), '')

  def get_reverse(self, query, expected_prop):
    if query.order_list():
      if query.order_size() > 1 or query.order(0).property() != expected_prop:
        raise BadRequest('Invalid order info')

      if query.order(0).direction() == query.order(0).DESCENDING:
        return True

    return False

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

  def group_filters(self, query):
    filter_props = []
    for query_filter in query.filter_list():
      if query_filter.property_size() != 1:
        raise BadRequest('Each filter must have exactly one property')

      prop = query_filter.property(0)
      filter_info = (query_filter.op(), prop.value())
      if prop.name() == filter_props[-1][0]:
        filter_props[-1][1].append(filter_info)
      else:
        filter_props.append((prop.name(), [filter_info]))

    for name, filters in filter_props[:-1]:
      if name == '__key__':
        raise BadRequest('Only the last filter property can be on __key__')

      if len(filters) != 1 or filters[0][0] != datastore_pb.Query_Filter.EQUAL:
        raise BadRequest('All but the last property must be equality filters')

  def get_iterator(self, tr, query):
    filter_props = self.group_filters(query)
    if not query.has_kind():
      index = KindlessIndex(query.app(), query.name_space(),
                            self._directory_cache)
      reverse = self.get_reverse(query, '__key__')
    elif all([name == '__key__' for name, _ in filter_props]):
      index = KindIndex(query.app(), query.name_space(), query.kind(),
                        self._directory_cache)
      reverse = self.get_reverse(query, '__key__')
    elif sum([name != '__key__' for name, _ in filter_props]) == 1:
      prop_name, filters = filter_props[0]
      directory_cache):
      index = SinglePropIndex(query.app(), query.name_space(), query.kind(), )
    else:
      raise BadRequest('Query is not supported')

    query_range = index.directory.range()
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
