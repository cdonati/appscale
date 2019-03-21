import fdb

fdb.api_version(600)

db = fdb.open()

ds_dir = fdb.directory.open(db, ('appscale', 'datastore'))


def print_data(tr, data_dir):
  namespaces = data_dir.list(tr)
  for namespace in namespaces:
    namespace_dir = data_dir.open(tr, (namespace,))
    print('/'.join(namespace_dir.get_path()) + ':')
    for kv in tr[namespace_dir.range()]:
      key_parts = namespace_dir.unpack(kv.key)
      value_parts = fdb.tuple.unpack(kv.value)
      print('{}: {}'.format(key_parts, value_parts))


def print_indexes(tr, indexes_dir):
  namespaces = indexes_dir.list(tr)
  for namespace in namespaces:
    print('---- namespace: "{}"'.format(namespace))


def main():
  tr = db.create_transaction()
  for project_id in ds_dir.list(tr):
    project_dir = ds_dir.open(tr, (project_id,))
    for section_id in project_dir.list(tr):
      section_dir = project_dir.open(tr, (section_id,))
      if section_id == 'data':
        print_data(tr, section_dir)

      if section_id == 'indexes':
        print_indexes(tr, section_dir)
