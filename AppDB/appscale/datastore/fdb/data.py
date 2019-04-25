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
from appscale.datastore.fdb.codecs import decode_path, decode_str, encode_path
from appscale.datastore.fdb.utils import (
  ABSENT_VERSION, EncodedTypes, fdb, put_chunks, KVIterator)

sys.path.append(APPSCALE_PYTHON_APPSERVER)
from google.appengine.datastore import entity_pb

logger = logging.getLogger(__name__)

MAX_ENTITY_SIZE = 1048572


class VersionEntry(object):
  __SLOTS__ = [u'project_id', u'namespace', u'path', u'commit_vs',
               u'encoded_entity', u'version']

  def __init__(self, project_id, namespace, path, commit_vs=None,
               encoded_entity=None, version=None):
    self.project_id = project_id
    self.namespace = namespace
    self.path = path
    self.commit_vs = commit_vs
    self.encoded_entity = encoded_entity
    self.version = ABSENT_VERSION if version is None else version

  @property
  def complete(self):
    return self.encoded_entity is not None

  @property
  def present(self):
    return self.version != ABSENT_VERSION

  @property
  def key(self):
    key = entity_pb.Reference()
    key.set_app(self.project_id)
    key.set_name_space(self.namespace)
    key.mutable_path().MergeFrom(decode_path(self.path))
    return key


class DataNamespace(object):
  DIR_NAME = u'data'

  # The max number of bytes for each FDB value.
  _CHUNK_SIZE = 10000

  # The number of bytes used to store an entity version.
  _VERSION_SIZE = 7

  # The number of bytes used to store a commit versionstamp.
  _VS_SIZE = 10

  # The number of bytes to use to encode the chunk index.
  _INDEX_SIZE = 1

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

    encoded_path = fdb.tuple.pack(path)
    if commit_vs is not None:
      prefix = self.directory.rawPrefix + encoded_path + commit_vs
      # All chunks for a given version.
      return slice(fdb.KeySelector.first_greater_or_equal(prefix + b'\x00'),
                   fdb.KeySelector.first_greater_than(prefix + b'\xff'))

    if read_vs is not None:
      path_prefix = self.directory.rawPrefix + encoded_path
      version_prefix = path_prefix + read_vs
      # All versions for a given path except those written after the read_vs.
      return slice(
        fdb.KeySelector.first_greater_or_equal(path_prefix + b'\x00'),
        fdb.KeySelector.first_greater_than(version_prefix + b'\xff'))

    prefix = self.directory.rawPrefix + encoded_path
    # All versions for a given path.
    return slice(fdb.KeySelector.first_greater_or_equal(prefix + b'\x00'),
                 fdb.KeySelector.first_greater_than(prefix + b'\xff'))

  def encode(self, path, entity, version, commit_vs=None):
    if isinstance(entity, entity_pb.EntityProto):
      entity = entity.Encode()

    if len(entity) > MAX_ENTITY_SIZE:
      raise BadRequest('Entity exceeds maximum size')

    encoded_version = struct.pack('<I', version)[:self._VERSION_SIZE]
    full_value = b''.join([encoded_version, EncodedTypes.ENTITY_V3, entity])
    chunk_count = int(math.ceil(len(full_value) / self._CHUNK_SIZE))
    return tuple(self._encode_kv(full_value, index, path, commit_vs)
                 for index in sm.range(chunk_count))

  def encode_key(self, path, commit_vs, index):
    encoded_vs = b'\x00' * self._VS_SIZE if commit_vs is None else commit_vs
    encoded_index = bytes(bytearray((index,)))
    encoded_key = b''.join([self.directory.rawPrefix, fdb.tuple.pack(path),
                            encoded_vs, encoded_index])
    if commit_vs is None:
      vs_index = len(encoded_key) - (self._VS_SIZE + self._INDEX_SIZE)
      encoded_key += struct.pack('<L', vs_index)

  def decode(self, kvs):
    path_slice = slice(len(self.directory.rawPrefix),
                       -1 * (self._VS_SIZE + self._INDEX_SIZE))
    vs_slice = slice(path_slice.stop, -1 * self._INDEX_SIZE)
    index_slice = slice(vs_slice.stop, None)
    path = fdb.tuple.unpack(kvs[0].key[path_slice])
    commit_vs = kvs[0].key[vs_slice]
    first_index = ord(kvs[0].key[index_slice])

    encoded_entity = None
    version = None
    if first_index == 0:
      encoded_val = b''.join([kv.value for kv in kvs])
      version = struct.unpack('<I', encoded_val[:self._VERSION_SIZE] + b'\x00')
      encoding = encoded_val[self._VERSION_SIZE]
      encoded_entity = encoded_val[self._VERSION_SIZE + 1:]
      if encoding != EncodedTypes.ENTITY_V3:
        raise InternalError(u'Unknown entity type')

    return VersionEntry(self.project_id, self.namespace, path, commit_vs,
                        encoded_entity, version)

  def _encode_kv(self, full_value, index, path, commit_vs):
    data_range = slice(index * self._CHUNK_SIZE,
                       (index + 1) * self._CHUNK_SIZE)
    encoded_val = full_value[data_range]
    return self.encode_key(path, commit_vs, index), encoded_val


class DataManager(object):
  GROUP_UPDATES_DIR = u'group-updates'

  def __init__(self, directory_cache, tornado_fdb):
    self._directory_cache = directory_cache
    self._tornado_fdb = tornado_fdb

  @gen.coroutine
  def get_latest(self, tr, key, read_vs=None, include_data=True):
    data_ns = DataNamespace.from_key(key, self._directory_cache)
    desired_slice = data_ns.get_slice(key.path(), read_vs=read_vs)
    last_version = yield self._last_version(
      tr, data_ns, desired_slice, include_data)
    if last_version is None:
      last_version = VersionEntry(data_ns.project_id, data_ns.namespace,
                                  encode_path(key.path()))

    raise gen.Return(last_version)

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
    data_ns = DataNamespace.from_key(key, self._directory_cache)
    desired_slice = data_ns.get_slice(key.path())
    last_entry = yield self._last_version(tr, data_ns, desired_slice,
                                          include_data=False)
    if last_entry is None:
      return

    raise gen.Return(last_entry.commit_vs)

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

    if not kvs:
      return

    last_kv = kvs[0]
    entry = data_ns.decode([last_kv])
    if not include_data or entry.complete:
      raise gen.Return(entry)

    version_slice = data_ns.get_slice(entry.path, entry.commit_vs)
    end_key = data_ns.encode_key(entry.path, entry.commit_vs, entry.index)
    remaining_slice = slice(version_slice.start,
                            fdb.KeySelector.first_greater_or_equal(end_key))
    kvs = self._get_range(tr, remaining_slice)
    raise gen.Return(data_ns.decode(kvs + [last_kv]))

  @gen.coroutine
  def _get_range(self, tr, data_range, snapshot=False):
    iterator = KVIterator(tr, self._tornado_fdb, data_range, snapshot=snapshot)
    all_kvs = []
    while True:
      kvs, more_results = yield iterator.next_page()
      all_kvs.extend(kvs)
      if not more_results:
        break

    raise gen.Return(all_kvs)
