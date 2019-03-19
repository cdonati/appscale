from appscale.datastore.fdb.utils import flat_path


class IndexManager(object):
  _INDEX_DIR = 'indexes'

  def __init__(self, directory_cache):
    self._directory_cache = directory_cache

  def put_entries(self, tr, old_entity, new_entity):
    # (appscale, datastore, <project>, indexes, <namespace>, <kind>, <prop name>, <prop type>)
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
      (project_id, self._INDEX_DIR, namespace, 'kinds', kind))
    if old_entity is None and new_entity is not None:
      tr.set_versionstamped_value(kind_index.pack((path[1:],)), '\xff' * 14)
    elif old_entity is not None and new_entity is None:
      del tr[kind_index.pack((path[1:],))]

    if old_entity is not None:
      for prop in old_entity.property_list():
        