"""
transactions: This maps transaction handles to metadata that the datastore
needs in order to handle operations for the transaction. Here are a few example
entries:

  ([directory^5], <handle id>, read_vs) -> <read versionstamp^6>
  ([directory], <handle>, lookups, <rpc versionstamp>) -> (<encoding>, <key>)

^5: A directory located at (appscale, datastore, <project>, transactions).
^6: Designates what version of the database read operations should see.
"""
import logging
import math
import sys

import six.moves as sm
from tornado import gen

from appscale.common.unpackaged import APPSCALE_PYTHON_APPSERVER
from appscale.datastore.dbconstants import (
  BadRequest, InternalError, MAX_GROUPS_FOR_XG, TooManyGroupsException)
from appscale.datastore.fdb.cache import SectionCache
from appscale.datastore.fdb.codecs import (
  decode_str, encode_path, encode_read_vs, encode_sortable_int,
  encode_vs_index)
from appscale.datastore.fdb.utils import (
  fdb, EncodedTypes, put_chunks, KVIterator, MAX_ENTITY_SIZE, VS_SIZE)

sys.path.append(APPSCALE_PYTHON_APPSERVER)
from google.appengine.datastore import datastore_pb, entity_pb

logger = logging.getLogger(__name__)


def decode_chunks(chunks, rpc_type):
  if rpc_type == MetadataKeys.PUTS:
    expected_encoding = EncodedTypes.ENTITY_V3
    pb_class = entity_pb.EntityProto
  elif rpc_type in (MetadataKeys.LOOKUPS, MetadataKeys.DELETES):
    expected_encoding = EncodedTypes.KEY_V3
    pb_class = entity_pb.Reference
  else:
    raise InternalError(u'Unexpected RPC type')

  elements = fdb.tuple.unpack(''.join(chunks))
  if elements[0] != expected_encoding:
    raise InternalError(u'Unexpected encoding')

  return [pb_class(encoded_value) for encoded_value in elements[1:]]


class TransactionMetadata(object):
  DIR_NAME = u'transactions'

  LOOKUPS = b'\x00'
  QUERIES = b'\x01'
  PUTS = b'\x02'
  DELETES = b'\x03'
  TASKS = b'\x04'

  # The max number of bytes for each FDB value.
  _CHUNK_SIZE = 10000

  _ENTITY_LEN_SIZE = 3

  def __init__(self, directory):
    self.directory = directory

  def encode_start_key(self, txid):
    return self._txid_prefix(txid)

  def encode_lookups(self, txid, keys):
    section_prefix = self._txid_prefix(txid) + self.LOOKUPS
    encoded_keys = [self._encode_ns_key(key) for key in keys]
    # TODO: Encode keys more efficiently.
    value = b''.join([b''.join([self._encode_entity_len(key), key])
                      for key in encoded_keys])
    return self._encode_chunks(section_prefix, value)

  def encode_query_key(self, txid, namespace, ancestor):
    if not isinstance(ancestor, tuple):
      ancestor = encode_path(ancestor)

    section_prefix = self._txid_prefix(txid) + self.QUERIES
    ns_path = fdb.tuple.pack((namespace,) + ancestor[:2])
    return section_prefix + ns_path

  def encode_puts(self, txid, entities):
    section_prefix = self._txid_prefix(txid) + self.PUTS
    encoded_entities = [entity.Encode() for entity in entities]
    value = b''.join([b''.join([self._encode_entity_len(entity), entity])
                      for entity in encoded_entities])
    return self._encode_chunks(section_prefix, value)

  def encode_deletes(self, txid, keys):
    section_prefix = self._txid_prefix(txid) + self.DELETES
    encoded_keys = [self._encode_ns_key(key) for key in keys]
    # TODO: Encode keys more efficiently.
    value = b''.join([b''.join([self._encode_entity_len(key), key])
                      for key in encoded_keys])
    return self._encode_chunks(section_prefix, value)

  def get_txid_slice(self, txid):
    prefix = self._txid_prefix(txid)
    return slice(fdb.KeySelector.first_greater_or_equal(prefix),
                 fdb.KeySelector.first_greater_than(prefix + b'\xff'))

  def _encode_ns_key(self, key):
    namespace = decode_str(key.name_space())
    return fdb.tuple.pack((namespace,) + encode_path(key.path()))

  def _txid_prefix(self, txid):
    scatter_byte = bytes(bytearray([txid % 256]))
    return self.directory.rawPrefix + scatter_byte + encode_read_vs(txid)

  def _encode_entity_len(self, encoded_entity):
    if len(encoded_entity) > MAX_ENTITY_SIZE:
      raise BadRequest(u'Entity exceeds maximum size')

    return encode_sortable_int(len(encoded_entity), self._ENTITY_LEN_SIZE)

  def _encode_chunks(self, section_prefix, value):
    full_prefix = section_prefix + b'\x00' * VS_SIZE
    vs_index = encode_vs_index(len(section_prefix))
    chunk_count = int(math.ceil(len(value) / self._CHUNK_SIZE))
    return tuple(
      (full_prefix + bytes(bytearray((index,))) + vs_index,
       value[index * self._CHUNK_SIZE:(index + 1) * self._CHUNK_SIZE])
      for index in sm.range(chunk_count))


class TransactionManager(object):
  def __init__(self, tornado_fdb, project_cache):
    self._tornado_fdb = tornado_fdb
    self._tx_metadata_cache = SectionCache(
      self._tornado_fdb, project_cache, TransactionMetadata)

  @gen.coroutine
  def create(self, tr, project_id):
    txid, tx_dir = yield [self._tornado_fdb.get_read_version(tr),
                          self._tx_metadata_cache.get(tr, project_id)]
    tr[tx_dir.encode_start_key(txid)] = b''
    raise gen.Return(txid)

  @gen.coroutine
  def log_lookups(self, tr, project_id, get_request):
    txid = get_request.transaction().handle()
    tx_dir = yield self._tx_metadata_cache.get(tr, project_id)
    for key, value in tx_dir.encode_lookups(txid, get_request.key_list()):
      tr.set_versionstamped_key(key, value)

  @gen.coroutine
  def log_query(self, tr, project_id, query):
    txid = query.transaction().handle()
    namespace = decode_str(query.name_space())
    if not query.has_ancestor():
      raise BadRequest(u'Queries in a transaction must specify an ancestor')

    tx_dir = yield self._tx_metadata_cache.get(tr, project_id)
    tr[tx_dir.encode_query_key(txid, namespace, query.ancestor())] = b''

  @gen.coroutine
  def log_puts(self, tr, project_id, put_request):
    txid = put_request.transaction().handle()
    tx_dir = yield self._tx_metadata_cache.get(tr, project_id)
    for key, value in tx_dir.encode_puts(txid, put_request.entity_list()):
      tr.set_versionstamped_key(key, value)

  @gen.coroutine
  def log_deletes(self, tr, project_id, delete_request):
    txid = delete_request.transaction().handle()
    tx_dir = yield self._tx_metadata_cache.get(tr, project_id)
    for key, value in tx_dir.encode_deletes(txid, delete_request.key_list()):
      tr.set_versionstamped_key(key, value)

  @gen.coroutine
  def delete(self, tr, project_id, txid):
    tx_dir = yield self._tx_metadata_cache.get(tr, project_id)
    del tr[tx_dir.get_txid_slice(txid)]

  @gen.coroutine
  def get_metadata(self, tr, project_id, txid):
    tx_dir = yield self._tx_metadata_cache.get(tr, project_id)
    iterator = KVIterator(tr, self._tornado_fdb, tx_dir.get_txid_slice(txid))
    all_kvs = []
    while True:
      kvs, more_results = yield iterator.next_page()
      all_kvs.extend(kvs)
      if not more_results:
        break

    if not all_kvs:
      raise BadRequest(u'Transaction not found')

    return tx_dir.decode_metadata(all_kvs)
    lookups = set()
    queried_groups = set()
    mutations = []

    tmp_chunks = []
    tmp_rpc_vs = None
    tmp_rpc_type = None

    while True:
      kvs, more_results = yield iterator.next_page()
      for kv in kvs:
        key_parts = metadata_subspace.unpack(kv.key)
        metadata_key = key_parts[0]
        if metadata_key == MetadataKeys.READ_VS:
          read_vs = kv.value
          continue

        if metadata_key == MetadataKeys.XG:
          xg = kv.value == XG
          continue

        if metadata_key == MetadataKeys.QUERIES:
          unpacked_value = fdb.tuple.unpack(kv.value)
          namespace = unpacked_value[0]
          group_path = unpacked_value[1:]
          queried_groups.add((namespace, group_path))
          continue

        rpc_vs = key_parts[1]
        if rpc_vs == tmp_rpc_vs:
          tmp_chunks.append(kv.value)
          continue

        if tmp_rpc_type == MetadataKeys.LOOKUPS:
          lookups.update(decode_chunks(tmp_chunks, tmp_rpc_type))
        elif tmp_rpc_type in (MetadataKeys.PUTS, MetadataKeys.DELETES):
          mutations.extend(decode_chunks(tmp_chunks, tmp_rpc_type))

        tmp_chunks = [kv.value]
        tmp_rpc_vs = rpc_vs
        tmp_rpc_type = metadata_key

      if not more_results:
        break

    if tmp_chunks and tmp_rpc_type == MetadataKeys.LOOKUPS:
      lookups.update(decode_chunks(tmp_chunks, tmp_rpc_type))
    elif tmp_chunks and tmp_rpc_type in (MetadataKeys.PUTS,
                                         MetadataKeys.DELETES):
      mutations.extend(decode_chunks(tmp_chunks, tmp_rpc_type))

    if read_vs is None or xg is None:

    lookup_groups = set()
    for key in lookups:
      group_path = encode_path(key.path())[:2]
      lookup_groups.add((key.name_space(), group_path))

    mutated_groups = set()
    for mutation in mutations:
      key = mutation
      if isinstance(mutation, entity_pb.EntityProto):
        key = mutation.key()

      group_path = encode_path(key.path())[:2]
      mutated_groups.add((key.name_space(), group_path))

    tx_groups = queried_groups | lookup_groups | mutated_groups
    max_groups = MAX_GROUPS_FOR_XG if xg else 1
    if len(tx_groups) > max_groups:
      raise TooManyGroupsException(u'Too many groups in transaction')

    raise gen.Return((read_vs, lookups, queried_groups, mutations))
