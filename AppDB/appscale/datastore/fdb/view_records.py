import argparse
import logging
import struct
import sys

import fdb
import six
import tabulate

from appscale.common.unpackaged import APPSCALE_PYTHON_APPSERVER
from appscale.datastore.fdb.codecs import decode_str, unpack_value
from appscale.datastore.fdb.indexes import (
  KindIndex, KindlessIndex, SinglePropIndex)

sys.path.append(APPSCALE_PYTHON_APPSERVER)
from google.appengine.api import datastore
from google.appengine.datastore import entity_pb

fdb.api_version(600)

db = fdb.open()

ds_dir = fdb.directory.open(db, ('appscale', 'datastore'))


def format_versionstamp(versionstamp):
  if not isinstance(versionstamp, fdb.tuple.Versionstamp):
    versionstamp = fdb.tuple.Versionstamp(versionstamp)

  if not versionstamp.is_complete():
    return None

  return struct.unpack('>Q', versionstamp.tr_version[:8])[0]


def format_path(path):
  index = 0
  elements = []
  while index < len(path):
    kind = path[index]
    id_or_name = path[index + 1]
    if isinstance(id_or_name, int):
      id_or_name = six.text_type(id_or_name)

    elements.append(':'.join([kind, id_or_name]))
    index += 2

  return '|'.join(elements)


def format_entity(encoded_entity):
  if not encoded_entity:
    return '(deleted)'

  entity_proto = entity_pb.EntityProto(encoded_entity)
  printable_entity = str(datastore.Entity.FromPb(entity_proto))
  if len(printable_entity) > 120:
    return printable_entity[:120] + '...'
  else:
    return printable_entity


def format_value(prop_value):
  encoded_type, value = unpack_value(prop_value)
  return value


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
  headers = ['Path', 'Versionstamp', 'Deleted VS']
  table = []

  for kv in tr[index.directory.range()]:
    entry = index.decode(kv)
    table.append([format_path(entry.path),
                  format_versionstamp(entry.commit_vs),
                  format_versionstamp(entry.deleted_vs)])

  print(tabulate.tabulate(table, headers=headers) + '\n')


def print_kind_indexes(tr, index_dir):
  for kind in index_dir.list(tr):
    index = KindIndex(index_dir.open(tr, (kind,)))
    print(str(index) + ':')

    headers = ['Path', 'Versionstamp', 'Deleted VS']
    table = []
    for kv in tr[index.directory.range()]:
      entry = index.decode(kv)
      table.append([format_path(entry.path),
                    format_versionstamp(entry.commit_vs),
                    format_versionstamp(entry.deleted_vs)])

    print(tabulate.tabulate(table, headers=headers) + '\n')


def print_single_prop_indexes(tr, index_dir):
  for kind in index_dir.list(tr):
    kind_dir = index_dir.open(tr, (kind,))
    for prop_name in kind_dir.list(tr):
      index = SinglePropIndex(kind_dir.open(tr, (prop_name,)))
      print(str(index) + ':')

      headers = ['Value', 'Path', 'Versionstamp', 'Deleted VS']
      table = []
      for kv in tr[index.directory.range()]:
        entry = index.decode(kv)
        table.append([format_value(entry.value),
                      format_path(entry.path),
                      format_versionstamp(entry.commit_vs),
                      format_versionstamp(entry.deleted_vs)])

      print(tabulate.tabulate(table, headers=headers) + '\n')


def print_composite_indexes(tr, index_dir):
  pass


def print_indexes(tr, indexes_dir, indexes):
  if not indexes:
    indexes = ['kindless', 'kind', 'single-property', 'composite']

  namespaces = indexes_dir.list(tr)
  for namespace in namespaces:
    namespace_dir = indexes_dir.open(tr, (namespace,))
    for index_type in namespace_dir.list(tr):
      index_dir = namespace_dir.open(tr, (index_type,))
      if index_type == 'kindless' and index_type in indexes:
        index = KindlessIndex(index_dir)
        print_kindless_index(tr, index)

      if index_type == 'kind' and index_type in indexes:
        print_kind_indexes(tr, index_dir)

      if index_type == 'single-property' and index_type in indexes:
        print_single_prop_indexes(tr, index_dir)

      if index_type == 'composite' and index_type in indexes:
        print_composite_indexes(tr, index_dir)


def main():
  logging.basicConfig()
  logging.getLogger('appscale').setLevel(logging.DEBUG)

  parser = argparse.ArgumentParser()
  parser.add_argument('--project')
  parser.add_argument('--indexes', nargs='*')
  args = parser.parse_args()

  tr = db.create_transaction()
  for project_id in ds_dir.list(tr):
    if args.project is not None and args.project != project_id:
      continue

    project_dir = ds_dir.open(tr, (project_id,))
    for section_id in project_dir.list(tr):
      section_dir = project_dir.open(tr, (section_id,))
      if section_id == 'data':
        print_data(tr, section_dir)

      if section_id == 'indexes':
        print_indexes(tr, section_dir, args.indexes)
