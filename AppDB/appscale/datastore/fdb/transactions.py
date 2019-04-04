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
import sys
import uuid

import six
from tornado import gen

from appscale.common.unpackaged import APPSCALE_PYTHON_APPSERVER
from appscale.datastore.dbconstants import (
  BadRequest, InternalError, MAX_GROUPS_FOR_XG, TooManyGroupsException)
from appscale.datastore.fdb.codecs import decode_str, encode_path
from appscale.datastore.fdb.utils import (
  fdb, EncodedTypes, put_chunks, KVIterator)

sys.path.append(APPSCALE_PYTHON_APPSERVER)
from google.appengine.datastore import datastore_pb, entity_pb

logger = logging.getLogger(__name__)


NOT_XG = b'\x00'
XG = b'\x01'


class MetadataKeys(object):
  READ_VS = b'\x00'
  XG = b'\x01'
  LOOKUPS = b'\x02'
  QUERIES = b'\x03'
  PUTS = b'\x04'
  DELETES = b'\x05'


def decode_chunks(chunks, rpc_type):
  if rpc_type == MetadataKeys.PUTS:
    expected_encoding = EncodedTypes.ENTITY_V3
    pb_class = entity_pb.EntityProto
  elif rpc_type in (MetadataKeys.LOOKUPS, MetadataKeys.DELETES):
    expected_encoding = EncodedTypes.KEY_V3
    pb_class = entity_pb.Reference
  else:
    raise InternalError('Unexpected RPC type')

  elements = fdb.tuple.unpack(''.join(chunks))
  if elements[0] != expected_encoding:
    raise InternalError('Unexpected encoding')

  return [pb_class(encoded_value) for encoded_value in elements[1:]]


class TransactionManager(object):
  DIRECTORY = u'transactions'

  def __init__(self, directory_cache, tornado_fdb):
    self._directory_cache = directory_cache
    self._tornado_fdb = tornado_fdb

  @gen.coroutine
  def create(self, tr, project_id, is_xg):
    txid = uuid.uuid4().int & (1 << 64) - 1
    tx_dir = self._directory_cache.get((project_id, self.DIRECTORY))
    read_vs_key = tx_dir.pack((txid, MetadataKeys.READ_VS))
    read_vs = yield self._tornado_fdb.get(tr, read_vs_key)
    if read_vs.present():
      raise InternalError('The datastore chose an existing txid')

    tr.set_versionstamped_value(read_vs_key, b'\x00' * 14)
    tr[tx_dir.pack((txid, MetadataKeys.XG))] = XG if is_xg else NOT_XG
    raise gen.Return(txid)

  def delete(self, tr, project_id, txid):
    tx_dir = self._directory_cache.get((project_id, self.DIRECTORY))
    del tr[tx_dir.range((txid,))]

  @gen.coroutine
  def get_read_vs(self, tr, project_id, txid):
    tx_dir = self._directory_cache.get((project_id, self.DIRECTORY))
    vs_key = tx_dir.pack((txid, MetadataKeys.READ_VS))
    read_vs = yield self._tornado_fdb.get(tr, vs_key)
    if not read_vs.present():
      raise BadRequest('Transaction does not exist')

    raise gen.Return(fdb.tuple.Versionstamp(read_vs.value))

  def log_rpc(self, tr, project_id, request):
    txid = request.transaction().handle()
    tx_dir = self._directory_cache.get((project_id, self.DIRECTORY))
    if isinstance(request, datastore_pb.PutRequest):
      value = fdb.tuple.pack(
        (EncodedTypes.ENTITY_V3,) +
        tuple(entity.Encode() for entity in request.entity_list()))
      subspace = tx_dir.subspace((txid, MetadataKeys.PUTS))
    elif isinstance(request, datastore_pb.GetRequest):
      value = fdb.tuple.pack(
        (EncodedTypes.KEY_V3,) +
        tuple(key.Encode() for key in request.key_list()))
      subspace = tx_dir.subspace((txid, MetadataKeys.LOOKUPS))
    elif isinstance(request, datastore_pb.DeleteRequest):
      value = fdb.tuple.pack(
        (EncodedTypes.KEY_V3,) +
        tuple(key.Encode() for key in request.key_list()))
      subspace = tx_dir.subspace((txid, MetadataKeys.DELETES))
    else:
      raise BadRequest('Unexpected RPC type')

    put_chunks(tr, value, subspace, add_vs=True)

  def log_query(self, tr, project_id, query):
    txid = query.transaction().handle()
    tx_dir = self._directory_cache.get((project_id, self.DIRECTORY))
    namespace = decode_str(query.name_space())
    if not query.has_ancestor():
      raise BadRequest('Queries in a transaction must specify an ancestor')

    group_path = encode_path(query.ancestor().path())[:2]
    key = tx_dir.pack_with_versionstamp(
      (txid, MetadataKeys.QUERIES, fdb.tuple.Versionstamp()))
    tr.set_versionstamped_key(key, fdb.tuple.pack((namespace,) + group_path))

  @gen.coroutine
  def get_metadata(self, tr, project_id, txid):
    tx_dir = self._directory_cache.get((project_id, self.DIRECTORY))
    metadata_subspace = tx_dir.subspace((txid,))

    read_vs = None
    xg = None
    lookups = set()
    queried_groups = set()
    mutations = []

    tmp_chunks = []
    tmp_rpc_vs = None
    tmp_rpc_type = None

    iterator = KVIterator(tr, self._tornado_fdb, metadata_subspace.range())
    while True:
      kvs, more_results = yield iterator.next_page()
      for kv in kvs:
        key_parts = metadata_subspace.unpack(kv.key)
        metadata_key = key_parts[0]
        if metadata_key == MetadataKeys.READ_VS:
          read_vs = fdb.tuple.Versionstamp(kv.value)
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
      raise BadRequest('Transaction not found')

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
      raise TooManyGroupsException('Too many groups in transaction')

    raise gen.Return((read_vs, lookups, queried_groups, mutations))
