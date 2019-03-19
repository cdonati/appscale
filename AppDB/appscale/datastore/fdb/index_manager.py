from collections import namedtuple

from appscale.datastore.fdb.utils import fdb, flat_path
from appscale.datastore.dbconstants import BadRequest

INDEX_DIR = 'indexes'


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

  def encode(self, value, path, commit_vs=fdb.tuple.Versionstamp()):
    return self.pack_method(commit_vs)((value,) + path + (commit_vs,))


class PropTypes(object):
  INT = '1'
  BOOL = '2'
  STRING = '3'
  DOUBLE = '4'


def unpack_value(value):
  if value.has_int64value():
    return PropTypes.INT, value.int64value()
  elif value.has_booleanvalue():
    return PropTypes.BOOL, value.booleanvalue()
  elif value.has_stringvalue():
    return PropTypes.STRING, value.stringvalue()
  elif value.has_doublevalue():
    return PropTypes.DOUBLE, value.doublevalue()
  else:
    raise BadRequest('Unknown PropertyValue type')


class IndexManager(object):
  _INDEX_DIR = 'indexes'

  def __init__(self, directory_cache):
    self._directory_cache = directory_cache

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
