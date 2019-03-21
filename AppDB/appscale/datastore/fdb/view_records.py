import fdb
import struct
import sys
import tabulate

fdb.api_version(600)

db = fdb.open()

ds_dir = fdb.directory.open(db, ('appscale', 'datastore'))


def format_path(path):
  index = 0
  elements = []
  while index < len(path):
    kind = path[index]
    id_or_name = path[index + 1]
    elements.append(':'.join([kind, str(id_or_name)]))
    index += 2

  return '|'.join(elements)


def print_data(tr, data_dir):
  namespaces = data_dir.list(tr)
  for namespace in namespaces:
    namespace_dir = data_dir.open(tr, (namespace,))
    project_id, section_id, pretty_ns = namespace_dir.get_path()[2:]
    if pretty_ns == '':
      pretty_ns = '""'

    print('/'.join([project_id, section_id, pretty_ns]) + ':')
    headers = ['Path', 'Versionstamp', 'Entity Version', 'Entity']
    table = []

    tmp_chunks = []
    for kv in tr[namespace_dir.range()]:
      key_parts = namespace_dir.unpack(kv.key)
      value_parts = fdb.tuple.unpack(kv.value)

      path = format_path(key_parts[:-2])
      versionstamp = struct.unpack('>Q', key_parts[-2].tr_version[:8])[0]
      entity_version = value_parts[0]
      entity_chunk = value_parts[2]
      index = key_parts[-1]
      if index != 0:
        tmp_chunks.append(entity_chunk)
      else:
        if tmp_chunks:
          encoded_entity = ''.join(tmp_chunks)
          table.append([path, versionstamp, entity_version, encoded_entity])
          tmp_chunks = [entity_chunk]

      if tmp_chunks:
          encoded_entity = ''.join(tmp_chunks)
          table.append([path, versionstamp, entity_version, encoded_entity])
          tmp_chunks = [entity_chunk]

      print(tabulate.tabulate(table, headers=headers))


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
