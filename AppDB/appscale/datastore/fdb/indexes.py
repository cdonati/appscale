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
  encode_ancestor_range, encode_path, encode_value, decode_element,
  decode_path, decode_value)
from appscale.datastore.fdb.utils import fdb, MAX_FDB_TX_DURATION, KVIterator
from appscale.datastore.dbconstants import BadRequest, InternalError
from appscale.datastore.index_manager import IndexInaccessible
from appscale.datastore.utils import _FindIndexToUse

sys.path.append(APPSCALE_PYTHON_APPSERVER)
from google.appengine.datastore import datastore_pb, entity_pb
from google.appengine.datastore.datastore_pb import Query_Filter, Query_Order

logger = logging.getLogger(__name__)

INDEX_DIR = u'indexes'

KEY_PROP = u'__key__'

START_FILTERS = (Query_Filter.GREATER_THAN_OR_EQUAL, Query_Filter.GREATER_THAN)
STOP_FILTERS = (Query_Filter.LESS_THAN_OR_EQUAL, Query_Filter.LESS_THAN)


def group_filters(query):
  filter_props = []
  for query_filter in query.filter_list():
    if query_filter.property_size() != 1:
      raise BadRequest(u'Each filter must have exactly one property')

    prop = query_filter.property(0)
    prop_name = six.text_type(prop.name())
    filter_info = (query_filter.op(), prop.value())
    if filter_props:
      last_filter_name, last_filter_list = filter_props[-1]
      if last_filter_name == prop_name:
        last_filter_list.append(filter_info)
    else:
      filter_props.append((prop_name, [filter_info]))

  for name, filters in filter_props[:-1]:
    if name == KEY_PROP:
      raise BadRequest(
        u'Only the last filter property can be on {}'.format(KEY_PROP))

    if len(filters) != 1 or filters[0][0] != Query_Filter.EQUAL:
      raise BadRequest(u'All but the last property must be equality filters')

  for prop_name, filters in filter_props:
    if len(filters) > 2:
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


def get_scan_direction(query, index):
  order_info = get_order_info(query)
  if not order_info:
    return Query_Order.ASCENDING

  first_property, first_direction = order_info[0]
  if first_property == KEY_PROP or isinstance(index, SinglePropIndex):
    return first_direction

  return next(direction for prop_name, direction in index.order_info
              if prop_name == first_property)


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
  def __init__(self, index, kv_iterator, read_vs=None):
    self.index = index
    self._kv_iterator = kv_iterator
    if read_vs is None:
      read_vs = fdb.tuple.Versionstamp()

    self._read_vs = read_vs
    self._done = False

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

  def get_slice(self, filter_props, ancestor_path=tuple()):
    subspace = self.directory
    start = None
    stop = None
    if ancestor_path:
      start, stop = encode_ancestor_range(subspace, ancestor_path)

    for prop_name, filters in filter_props:
      if prop_name != KEY_PROP:
        raise BadRequest(u'Unexpected filter: {}'.format(prop_name))

      if len(filters) == 1:
        op, value = filters[0]
        if op == Query_Filter.EQUAL:
          subspace = subspace.subspace(self.encode_path(value))
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
    dir_repr = u'/'.join([self.project_id, repr(self.namespace), self.kind])
    return u'KindIndex({})'.format(dir_repr)

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

  def __repr__(self):
    dir_repr = u'/'.join([self.project_id, repr(self.namespace), self.kind,
                          self.prop_name])
    return u'SinglePropIndex({})'.format(dir_repr)

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

  def get_slice(self, filter_props, ancestor_path=tuple()):
    subspace = self.directory
    start = None
    stop = None
    if ancestor_path:
      # Apply property equality first if it exists.
      prop_name, filters = filter_props[0]
      if len(filters) == 1:
        op, value = filters[0]
        if op == Query_Filter.EQUAL:
          subspace = subspace.subspace((encode_value(value),))
          filter_props = filter_props[1:]

      start, stop = encode_ancestor_range(subspace, ancestor_path)

    for prop_name, filters in filter_props:
      if prop_name == self.prop_name:
        encoder = encode_value
      elif prop_name == KEY_PROP:
        encoder = self.encode_path
      else:
        raise BadRequest(u'Unexpected filter: {}'.format(prop_name))

      if len(filters) == 1:
        op, value = filters[0]
        if op == Query_Filter.EQUAL:
          subspace = subspace.subspace((encoder(value),))
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
    components = [self.project_id, repr(self.namespace), self.kind]
    if self.ancestor:
      components.append(u'(includes ancestors)')

    for prop_name, direction in self.order_info:
      if direction == Query_Order.DESCENDING:
        prop_name = '-' + prop_name

      components.append(prop_name)

    return u'CompositeIndex({})'.format(u'/'.join(components))

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
    encoded_values = itertools.product(*encoded_values_by_prop)
    if not self.ancestor:
      return tuple(pack((value, self.encode_path(path), commit_vs))
                   for value in encoded_values)

    keys = []
    for index in range(2, len(path), 2):
      ancestor_path = path[:index]
      remaining_path = self.encode_path(path[index:])
      keys.extend(
        [pack((ancestor_path, value, remaining_path, commit_vs))
         for value in encoded_values])

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

  def get_slice(self, filter_props, ancestor_path=tuple()):
    subspace = self.directory.subspace(ancestor_path)
    start = None
    stop = None
    for prop_name, filters in filter_props:
      index_direction = next(direction for name, direction in self.order_info)
      reverse = index_direction == Query_Order.DESCENDING
      if prop_name in self.prop_names:
        encoder = lambda val: encode_value(val, reverse)
      elif prop_name == KEY_PROP:
        encoder = self.encode_path
      else:
        raise BadRequest(u'Unexpected filter: {}'.format(prop_name))

      if len(filters) == 1:
        op, value = filters[0]
        if op == Query_Filter.EQUAL:
          subspace = subspace.subspace((encoder(value),))
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
    index = self._get_perfect_index(query)
    if index is None:
      raise BadRequest('Query not supported')

    filter_info = group_filters(query)
    if query.has_ancestor():
      ancestor_path = encode_path(query.ancestor().path())
      desired_slice = index.get_slice(filter_info, ancestor_path)
    else:
      desired_slice = index.get_slice(filter_info)

    reverse = get_scan_direction(query, index) == Query_Order.DESCENDING
    rpc_limit, check_more_results = self.rpc_limit(query)
    fetch_limit = rpc_limit
    if check_more_results:
      fetch_limit += 1

    kv_iterator = KVIterator(tr, self._tornado_fdb, desired_slice, fetch_limit,
                             reverse, snapshot=True)
    logger.debug('using index: {}'.format(index))
    logger.debug('kv_iterator: {}'.format(kv_iterator))
    iterator = IndexIterator(index, kv_iterator, read_vs)

    return iterator

  @gen.coroutine
  def update_composite_index(self, project_id, index_pb, cursor=(None, None)):
    start_ns, start_key = cursor
    kind = six.text_type(index_pb.definition().entity_type())
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
      logger.info('Backfilling {}'.format(composite_index))
      try:
        kind_index_dir = indexes_dir.open(
          tr, (namespace, KindIndex.DIR_NAME, kind))
      except ValueError:
        logger.info('No entities exist for {}'.format(composite_index))
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
          logger.info('Finished backfilling {}'.format(composite_index))
          break

        if time.time() > deadline:
          try:
            yield self._tornado_fdb.commit(tr)
            cursor = (namespace, kvs[-1].key)
          except fdb.FDBError as fdb_error:
            logger.warning('Error while updating index: {}'.format(fdb_error))
            tr.on_error(fdb_error).wait()

          yield self.update_composite_index(project_id, index_pb, cursor)
          return

  def _get_index_keys(self, entity, commit_vs=None):
    if commit_vs is None:
      commit_vs = fdb.tuple.Versionstamp()

    project_id = six.text_type(entity.key().app())
    namespace = six.text_type(entity.key().name_space())
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
      prop_name = six.text_type(prop.name())
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
        prop_name = next(prop_name for prop_name in prop_names
                         if prop_name != KEY_PROP)
        return SinglePropIndex.from_cache(
          project_id, namespace, six.text_type(query.kind()), prop_name,
          self._directory_cache)

    index_pb = _FindIndexToUse(query, self._get_indexes_pb(project_id))
    if index_pb is not None:
      order_info = ((prop.name(), prop.direction())
                    for prop in index_pb.definition().property_list())
      return CompositeIndex.from_cache(
        project_id, namespace, index_pb.id(), kind,
        index_pb.definition().ancestor(), order_info, self._directory_cache)

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
