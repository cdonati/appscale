import fdb
import logging
import struct
import sys
import tabulate

from appscale.common.unpackaged import APPSCALE_PYTHON_APPSERVER
from appscale.datastore.fdb.index_manager import (
  get_type, KindIndex, KindlessIndex, SinglePropIndex)

sys.path.append(APPSCALE_PYTHON_APPSERVER)
from google.appengine.api import datastore
from google.appengine.datastore import entity_pb

fdb.api_version(600)

db = fdb.open()

ds_dir = fdb.directory.open(db, ('appscale', 'datastore'))


def format_versionstamp(versionstamp):
  return struct.unpack('>Q', versionstamp.tr_version[:8])[0]


def format_path(path):
  print('path: {}'.format(path))
  index = 0
  elements = []
  while index < len(path):
    kind = path[index]
    id_or_name = path[index + 1]
    elements.append(':'.join([kind, str(id_or_name)]))
    index += 2

  return '|'.join(elements)


def format_entity(encoded_entity):
  entity_proto = entity_pb.EntityProto(encoded_entity)
  return datastore.Entity.FromPb(entity_proto)


def format_value(value):
  type_name, encoded_type = get_type(value)
  return repr(getattr(value, '{}value'.format(type_name))())


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
    path = None
    versionstamp = None
    entity_version = None
    for kv in tr[namespace_dir.range()]:
      key_parts = namespace_dir.unpack(kv.key)
      value_parts = fdb.tuple.unpack(kv.value)

      entity_chunk = value_parts[2]
      index = key_parts[-1]
      if index != 0:
        tmp_chunks.append(entity_chunk)
        continue
      else:
        if tmp_chunks:
          entity = format_entity(''.join(tmp_chunks))
          table.append([path, versionstamp, entity_version, entity])

        tmp_chunks = [entity_chunk]
        path = format_path(key_parts[:-2])
        versionstamp = format_versionstamp(key_parts[-2])
        entity_version = value_parts[0]

    if tmp_chunks:
      entity = format_entity(''.join(tmp_chunks))
      table.append([path, versionstamp, entity_version, entity])

    print(tabulate.tabulate(table, headers=headers) + '\n')


def print_kindless_index(tr, index):
  print(str(index) + ':')
  headers = ['Path', 'Versionstamp']
  table = []

  for kv in tr[index.directory.range()]:
    entry = index.decode(kv)
    table.append([format_path(entry.path),
                  format_versionstamp(entry.commit_vs)])

  print(tabulate.tabulate(table, headers=headers) + '\n')


def print_kind_indexes(tr, index_dir):
  for kind in index_dir.list(tr):
    index = KindIndex(index_dir.open(tr, (kind,)))
    print(str(index) + ':')

    headers = ['Path', 'Versionstamp']
    table = []
    for kv in tr[index.directory.range()]:
      entry = index.decode(kv)
      table.append([format_path(entry.path),
                    format_versionstamp(entry.commit_vs)])

    print(tabulate.tabulate(table, headers=headers) + '\n')


def print_single_prop_indexes(tr, index_dir):
  for kind in index_dir.list(tr):
    kind_dir = index_dir.open(tr, (kind,))
    for prop_name in kind_dir.list(tr):
      index = SinglePropIndex(kind_dir.open(tr, (prop_name,)))
      print(str(index) + ':')

      headers = ['Value', 'Path', 'Versionstamp']
      table = []
      for kv in tr[index.directory.range()]:
        entry = index.decode(kv)
        table.append([format_value(entry.value),
                      format_path(entry.path),
                      format_versionstamp(entry.commit_vs)])

      print(tabulate.tabulate(table, headers=headers) + '\n')


def print_indexes(tr, indexes_dir):
  namespaces = indexes_dir.list(tr)
  for namespace in namespaces:
    namespace_dir = indexes_dir.open(tr, (namespace,))
    for index_type in namespace_dir.list(tr):
      index_dir = namespace_dir.open(tr, (index_type,))
      if index_type == 'kindless':
        index = KindlessIndex(index_dir)
        print_kindless_index(tr, index)

      if index_type == 'kind':
        print_kind_indexes(tr, index_dir)

      if index_type == 'single-property':
        print_single_prop_indexes(tr, index_dir)


def main():
  logging.getLogger('appscale').setLevel(logging.DEBUG)
  tr = db.create_transaction()
  for project_id in ds_dir.list(tr):
    project_dir = ds_dir.open(tr, (project_id,))
    for section_id in project_dir.list(tr):
      section_dir = project_dir.open(tr, (section_id,))
      if section_id == 'data':
        print_data(tr, section_dir)

      if section_id == 'indexes':
        print_indexes(tr, section_dir)
