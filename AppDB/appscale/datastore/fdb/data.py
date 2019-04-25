"""
data: This maps entity keys to encoded entity data. The data is a tuple that
specifies the version and the encoding. Due to FDB's value size limit, data
that exceeds the chunk size threshold is split into multiple key-values. The
index value indicates the position of the chunk. Here is an example template
along with an example key-value:

  ([directory^1], [path]^2, <vs^3>, <index>) -> (<version>, <encoding>, <data>)
  ([directory], Greeting, 476633..., <vs>, 0) -> (0, 155284..., <protobuffer>)

^1: A directory located at (appscale, datastore, <project>, data, <namespace>).
^2: Items wrapped in "[]" represent multiple elements for brevity.
^3: A FoundationDB-generated value specifying the commit versionstamp. It is
    used for enforcing consistency.
"""
from __future__ import division
import logging
import math
import struct
import sys

import six.moves as sm
from tornado import gen

from appscale.common.unpackaged import APPSCALE_PYTHON_APPSERVER
from appscale.datastore.dbconstants import BadRequest, InternalError
from appscale.datastore.fdb.codecs import decode_str, encode_path
from appscale.datastore.fdb.utils import (
  ABSENT_VERSION, EncodedTypes, fdb, put_chunks, KVIterator)

sys.path.append(APPSCALE_PYTHON_APPSERVER)
from google.appengine.datastore import entity_pb

logger = logging.getLogger(__name__)

MAX_ENTITY_SIZE = 1048572


class VersionEntry(object):
  __SLOTS__ = [u'project_id', u'namespace', u'path', u'commit_vs',
               u'encoded_entity']
  def __init__(self, project_id, namespace, path, commit_vs,
               encoded_entity=None, version=None):
    self.project_id = project_id
    self.namespace = namespace
    self.path = path
    self.commit_vs = commit_vs
    self.encoded_entity = encoded_entity
    self.version = version

  @property
  def complete(self):
    return self.encoded_entity is not None


class NamespaceData(object):
  DIR_NAME = u'data'

  # The max number of bytes for each FDB value.
  _CHUNK_SIZE = 10000

  def __init__(self, directory):
    self.directory = directory

  @classmethod
  def from_cache(cls, project_id, namespace, directory_cache):
    directory = directory_cache.get((project_id, cls.DIR_NAME, namespace))
    return cls(directory)

  @classmethod
  def from_key(cls, key, directory_cache):
    project_id = decode_str(key.app())
    namespace = decode_str(key.name_space())
    return cls.from_cache(project_id, namespace, directory_cache)

  @property
  def project_id(self):
    return self.directory.get_path()[2]

  @property
  def namespace(self):
    return self.directory.get_path()[3]

  def get_slice(self, path, commit_vs=None, read_vs=None):
    if not isinstance(path, tuple):
      path = encode_path(path)

    path_subspace = self.directory.subspace((path,))
    if read_vs is not None:
      # Ignore values written after read_vs.
      start = fdb.KeySelector.first_greater_than(path_subspace.range().start)
      stop = fdb.KeySelector.first_greater_or_equal(
        path_subspace.range((read_vs,)).stop)
      return slice(start, stop)

    if commit_vs is None:
      return self.directory.range((path,))

    # TODO
    return

  def encode(self, path, entity, version, commit_vs=None):
    if isinstance(entity, entity_pb.EntityProto):
      entity = entity.Encode()

    if len(entity) > MAX_ENTITY_SIZE:
      raise BadRequest('Entity exceeds maximum size')

    encoded_version = struct.pack('<I', version)[:7]
    full_value = b''.join([encoded_version, EncodedTypes.ENTITY_V3, entity])
    chunk_count = int(math.ceil(len(full_value) / self._CHUNK_SIZE))
    return tuple(self._encode_kv(full_value, index, path, commit_vs)
                 for index in sm.range(chunk_count))

  def decode(self, kvs):
    encoded_path = kvs[0].key[len()]
    path, commit_vs, first_index = self.directory.unpack(kvs[0].key)
    if first_index == 0:
      encoded_data = b''.join([kv.value for kv in kvs])
      version, encoding, encoded_entity = fdb.tuple.unpack(''.join(chunks))
      if encoding != EncodedTypes.ENTITY_V3:
        raise InternalError(u'Unknown entity type')

      return version, encoded_entity

    commit_vs = None
    for kv in kvs:
      path, commit_vs, index = self.directory.unpack(kv.key)
      commit_vs = unpacked_key[-2]
      data_index = unpacked_key[-1]
      raise gen.Return((chunk, commit_vs, data_index))

  def _encode_kv(self, full_value, index, path, commit_vs):
    encoded_vs = b'\x00' * 10 if commit_vs is None else commit_vs
    encoded_index = bytes(bytearray((index,)))
    encoded_key = b''.join([self.directory.rawPrefix, fdb.tuple.pack(path),
                            encoded_vs, encoded_index])
    data_range = slice(index * self._CHUNK_SIZE,
                       (index + 1) * self._CHUNK_SIZE)
    encoded_val = full_value[data_range]
    if commit_vs is None:
      vs_index = len(encoded_key) - len(encoded_index) - len(encoded_vs)
      encoded_key += struct.pack('<L', vs_index)

    return encoded_key, encoded_val

class DataManager(object):
  GROUP_UPDATES_DIR = u'group-updates'

  def __init__(self, directory_cache, tornado_fdb):
    self._directory_cache = directory_cache
    self._tornado_fdb = tornado_fdb

  @gen.coroutine
  def get_latest(self, tr, key, read_vs=None):
    ns_data = NamespaceData.from_key(key, self._directory_cache)
    path_range = ns_data.get_slice(key.path())

    path_subspace = self._subspace_from_key(key)
    last_chunk, commit_vs, data_index = yield self._last_chunk(
      tr, path_subspace, read_vs)
    if last_chunk is None:
      raise gen.Return((key, None, ABSENT_VERSION, commit_vs))

    # If the retrieved kv does not contain the whole entity, fetch it.
    if data_index > 0:
      commit_subspace = path_subspace.subspace((commit_vs,))
      remaining_range = slice(commit_subspace.start,
                              path_subspace.pack((commit_vs, data_index)))
      initial_chunks = yield self._get_range(tr, remaining_range)
      chunks = initial_chunks + [last_chunk]
    else:
      chunks = [last_chunk]

    version, encoded_entity = from_chunks(chunks)
    raise gen.Return((key, encoded_entity, version, commit_vs))

  @gen.coroutine
  def get_entry(self, tr, entry, snapshot=False):
    data_ns_dir = self._directory_cache.get(
      (entry.project_id, self.DATA_DIR, entry.namespace))
    data_range = data_ns_dir.range(entry.path + (entry.commit_vs,))
    chunks = yield self._get_range(tr, data_range, snapshot)
    raise gen.Return(from_chunks(chunks))

  @gen.coroutine
  def get_version(self, tr, key, commit_vs):
    path_subspace = self._subspace_from_key(key)
    vs_subspace = path_subspace.subspace((commit_vs,))
    chunks = yield self._get_range(tr, vs_subspace.range())
    raise gen.Return(from_chunks(chunks))

  @gen.coroutine
  def get_version_from_path(self, tr, project_id, namespace, path, commit_vs):
    directory = self._directory_cache.get(
      (project_id, self.DATA_DIR, namespace))
    vs_subspace = directory.subspace(path + (commit_vs,))
    chunks = yield self._get_range(tr, vs_subspace.range())
    raise gen.Return(from_chunks(chunks))

  @gen.coroutine
  def latest_vs(self, tr, key):
    path_subspace = self._subspace_from_key(key)
    _, commit_vs, _ = yield self._last_chunk(tr, path_subspace)
    raise gen.Return(commit_vs)

  @gen.coroutine
  def last_commit(self, tr, project_id, namespace, group_path):
    group_ns_dir = self._directory_cache.get(
      (project_id, self.GROUP_UPDATES_DIR, namespace))
    group_key = group_ns_dir.pack(group_path)
    last_updated_vs = yield self._tornado_fdb.get(tr, group_key)
    if not last_updated_vs.present():
      return

    raise gen.Return(fdb.tuple.Versionstamp(last_updated_vs))

  def put(self, tr, key, version, encoded_entity):
    path_subspace = self._subspace_from_key(key)
    encoded_value = fdb.tuple.pack((version, EncodedTypes.ENTITY_V3,
                                    encoded_entity))
    put_chunks(tr, encoded_value, path_subspace, add_vs=True)
    tr.set_versionstamped_value(self._group_key(key), b'\x00' * 14)

  def hard_delete(self, tr, key, commit_vs):
    """ Only the GC should use this. """
    path_subspace = self._subspace_from_key(key)
    version_subspace = path_subspace.subspace((commit_vs,))
    del tr[version_subspace.range()]

  def _group_key(self, key):
    project_id = decode_str(key.app())
    namespace = decode_str(key.name_space())
    group_path = encode_path(key.path())[:2]
    group_ns_dir = self._directory_cache.get(
      (project_id, self.GROUP_UPDATES_DIR, namespace))
    return group_ns_dir.pack(group_path)

  def _subspace_from_key(self, key):
    project_id = decode_str(key.app())
    namespace = decode_str(key.name_space())
    path = encode_path(key.path())
    data_ns_dir = self._directory_cache.get(
      (project_id, self.DATA_DIR, namespace))
    return data_ns_dir.subspace(path)

  @gen.coroutine
  def _last_version(self, tr, data_ns, desired_slice, include_data=True):
    kvs, count, more_results = yield self._tornado_fdb.get_range(
      tr, desired_slice, limit=1, reverse=True)

    if not count:
      return

    entry = data_ns.decode(kvs)
    if not include_data or entry.complete:
      raise gen.Return(entry)

    chunk = kvs[0].value
    unpacked_key = path_subspace.unpack(kvs[0].key)
    commit_vs = unpacked_key[-2]
    data_index = unpacked_key[-1]
    raise gen.Return((chunk, commit_vs, data_index))

  @gen.coroutine
  def _get_range(self, tr, data_range, snapshot=False):
    iterator = KVIterator(tr, self._tornado_fdb, data_range, snapshot=snapshot)
    chunks = []
    while True:
      kvs, more_results = yield iterator.next_page()
      chunks.extend([kv.value for kv in kvs])
      if not more_results:
        break

    raise gen.Return(chunks)
