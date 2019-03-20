import sys

from tornado import gen

from appscale.common.unpackaged import APPSCALE_PYTHON_APPSERVER
from appscale.datastore.fdb.utils import (
  decode_path, fdb, flat_path, RangeIterator)
from appscale.datastore.dbconstants import BadRequest

sys.path.append(APPSCALE_PYTHON_APPSERVER)
from google.appengine.datastore import entity_pb

INDEX_DIR = 'indexes'

class PropertyTypes(object):
  INT_64 = 'int64'
  BOOLEAN = 'boolean'
  STRING = 'string'
  DOUBLE = 'double'
  REFERENCE = 'reference'


class Index(object):
  __SLOTS__ = ['directory']

  def __init__(self, directory):
    self.directory = directory

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

  def encode(self, path, commit_vs=fdb.tuple.Versionstamp()):
    return self.pack_method(commit_vs)(path + (commit_vs,))


class KindIndex(Index):
  DIR_NAME = 'kind'

  def __init__(self, project_id, namespace, kind, directory_cache):
    directory = directory_cache.get(
      (project_id, INDEX_DIR, namespace, self.DIR_NAME, kind))
    super(KindIndex, self).__init__(directory)

  def encode(self, path, commit_vs=fdb.tuple.Versionstamp()):
    return self.pack_method(commit_vs)(path + (commit_vs,))


class SinglePropIndex(Index):
  DIR_NAME = 'single-property'

  def __init__(self, project_id, namespace, kind, prop_name, prop_type,
               directory_cache):
    directory = directory_cache.get(
      (project_id, INDEX_DIR, namespace, self.DIR_NAME, kind, prop_name,
       prop_type))
    super(SinglePropIndex, self).__init__(directory)

  @property
  def type(self):
    return self.directory.get_path()[-1]

  def encode(self, value, path, commit_vs=fdb.tuple.Versionstamp()):
    if self.type == PropertyTypes.REFERENCE:
      # The delimeter allows the decoder to differentiate between the property
      # value and the entity path.
      delimiter = '\x00'
      return self.pack_method(commit_vs)(value + (delimiter,) + path +
                                         (commit_vs,))
    else:
      return self.pack_method(commit_vs)((value,) + path + (commit_vs,))


def unpack_value(value):
  if value.has_int64value():
    return PropertyTypes.INT_64, value.int64value()
  elif value.has_booleanvalue():
    return PropertyTypes.BOOLEAN, value.booleanvalue()
  elif value.has_stringvalue():
    return PropertyTypes.STRING, value.stringvalue()
  elif value.has_doublevalue():
    return PropertyTypes.DOUBLE, value.doublevalue()
  elif value.has_referencevalue():
    ref = value.referencevalue()
    value_tuple = (ref.app(), ref.name_space()) + flat_path(ref)
    return PropertyTypes.REFERENCE, value_tuple

  raise BadRequest('Unknown PropertyValue type')


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
        type_, value = unpack_value(prop.value())
        index = SinglePropIndex(project_id, namespace, kind, prop.name(),
                                type_, self._directory_cache)
        del tr[index.encode(value, path, old_vs)]

    if new_entity is not None:
      tr.set_versionstamped_key(kindless_index.encode(path), '')
      tr.set_versionstamped_key(kind_index.encode(path), '')
      for prop in new_entity.property_list():
        type_, value = unpack_value(prop.value())
        index = SinglePropIndex(project_id, namespace, kind, prop.name(),
                                type_, self._directory_cache)
        tr.set_versionstamped_key(index.encode(value, path), '')

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

  @gen.coroutine
  def kindless_query(self, tr, query):
    project_id = query.app()
    namespace = query.name_space()
    index = KindlessIndex(project_id, namespace, self._directory_cache)
    reverse = self.get_reverse(query, '__key__')
    rpc_limit, check_more_results = self.rpc_limit(query)
    fetch_limit = rpc_limit
    if check_more_results:
      fetch_limit += 1

    more_results = False
    iterator = RangeIterator(tr, self._tornado_fdb, index.directory.range(),
                             fetch_limit, reverse, snapshot=True)
    if query.property_name_list():
      if query.property_name(0) != '__key__' or query.property_name_size() > 1:
        raise BadRequest('Invalid property name list')

      results = []
      while True:
        kvs, more_iterator_results = yield iterator.next_page()
        for kv in kvs:
          results.append(index.directory.unpack(kv.key)[:-1])

        if not more_iterator_results:
          break

      for page, more_results in

    if iterator is None:
      raise BadRequest('Query not supported')
