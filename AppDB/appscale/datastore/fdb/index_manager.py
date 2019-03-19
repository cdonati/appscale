from appscale.datastore.fdb.utils import flat_path
from appscale.datastore.dbconstants import BadRequest


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

  def put_entries(self, tr, old_entity, new_entity):
    # (appscale, datastore, <project>, indexes, <namespace>, single-property,
    # <kind>, <prop name>, <prop type>)
    # ([index dir^4], <property value>, [entity path], <commit versionstamp>) -> ''
    project_id = new_entity.key().app()
    namespace = new_entity.key().name_space()
    path = flat_path(new_entity.key())
    kind = path[0]

    kindless_index = self._directory_cache.get(
      (project_id, self._INDEX_DIR, namespace, 'kindless'))
    if old_entity is None and new_entity is not None:
      tr.set_versionstamped_value(kindless_index.pack((path,)), '\xff' * 14)
    elif old_entity is not None and new_entity is None:
      del tr[kindless_index.pack((path,))]

    kind_index = self._directory_cache.get(
      (project_id, self._INDEX_DIR, namespace, 'kind', kind))
    if old_entity is None and new_entity is not None:
      tr.set_versionstamped_value(kind_index.pack((path[1:],)), '\xff' * 14)
    elif old_entity is not None and new_entity is None:
      del tr[kind_index.pack((path[1:],))]

    if old_entity is not None:
      for prop in old_entity.property_list():
        type_, val = unpack_value(prop.value())
        prop_index = self._directory_cache.get(
          (project_id, self._INDEX_DIR, namespace, 'single-property', kind,
           prop.name(), type_))
        del tr[prop_index.pack((val,) + path)]