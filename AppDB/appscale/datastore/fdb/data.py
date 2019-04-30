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
  ABSENT_VERSION, EncodedTypes, fdb, hash_tuple, KVIterator, VS_SIZE)

sys.path.append(APPSCALE_PYTHON_APPSERVER)
from google.appengine.datastore import entity_pb

logger = logging.getLogger(__name__)

MAX_ENTITY_SIZE = 1048572


class VersionEntry(object):
  __SLOTS__ = [u'project_id', u'namespace', u'path', u'commit_vs',
               u'version', u'_encoded_entity', u'_decoded_entity']

  def __init__(self, project_id, namespace, path, commit_vs=None,
               encoded_entity=None, version=None):
    self.project_id = project_id
    self.namespace = namespace
    self.path = path
    self.commit_vs = commit_vs
    self.version = ABSENT_VERSION if version is None else version
    self._encoded_entity = encoded_entity
    self._decoded_entity = None

  @property
  def complete(self):
    return self._encoded_entity is not None or self._decoded_entity is not None

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

  @property
  def encoded(self):
    if self._encoded_entity is not None:
      return self._encoded_entity
    elif self._decoded_entity is not None:
      self._encoded_entity = self._decoded_entity.Encode()
      return self._encoded_entity
    else:
      return None

  @property
  def decoded(self):
    if self._decoded_entity is not None:
      return self._decoded_entity
    elif self._encoded_entity is not None:
      self._decoded_entity = entity_pb.EntityProto(self._encoded_entity)
      return self._decoded_entity
    else:
      return None


class DataNamespace(object):
  DIR_NAME = u'data'

  # The max number of bytes for each FDB value.
  _CHUNK_SIZE = 10000

  # The number of bytes used to store an entity version.
  _VERSION_SIZE = 7

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
    return self.directory.get_path()[4]

  @property
  def prefix_size(self):
    # The length of the directory prefix and the scatter byte.
    return len(self.directory.rawPrefix) + 1

  def get_slice(self, path, commit_vs=None, read_vs=None):
    if not isinstance(path, tuple):
      path = encode_path(path)

    path_prefix = b''.join([self.directory.rawPrefix, hash_tuple(path),
                            fdb.tuple.pack(path)])
    if commit_vs is not None:
      prefix = path_prefix + commit_vs
      # All chunks for a given version.
      return slice(fdb.KeySelector.first_greater_or_equal(prefix + b'\x00'),
                   fdb.KeySelector.first_greater_than(prefix + b'\xff'))

    if read_vs is not None:
      version_prefix = path_prefix + read_vs
      # All versions for a given path except those written after the read_vs.
      return slice(
        fdb.KeySelector.first_greater_or_equal(path_prefix + b'\x00'),
        fdb.KeySelector.first_greater_than(version_prefix + b'\xff'))

    # All versions for a given path.
    return slice(fdb.KeySelector.first_greater_or_equal(path_prefix + b'\x00'),
                 fdb.KeySelector.first_greater_than(path_prefix + b'\xff'))

  def encode(self, path, entity, version, commit_vs=None):
    if isinstance(entity, entity_pb.EntityProto):
      entity = entity.Encode()

    if len(entity) > MAX_ENTITY_SIZE:
      raise BadRequest(u'Entity exceeds maximum size')

    encoded_version = struct.pack('<Q', version)
    if any(byte != b'\x00' for byte in encoded_version[self._VERSION_SIZE:]):
      raise InternalError(u'Version exceeds maximum size')

    encoded_version = encoded_version[self._VERSION_SIZE]
    full_value = b''.join([encoded_version, EncodedTypes.ENTITY_V3, entity])
    chunk_count = int(math.ceil(len(full_value) / self._CHUNK_SIZE))
    return tuple(self._encode_kv(full_value, index, path, commit_vs)
                 for index in sm.range(chunk_count))

  def encode_key(self, path, commit_vs, index):
    encoded_vs = b'\x00' * VS_SIZE if commit_vs is None else commit_vs
    encoded_index = bytes(bytearray((index,)))
    encoded_key = b''.join([self.directory.rawPrefix, hash_tuple(path),
                            fdb.tuple.pack(path), encoded_vs, encoded_index])
    if commit_vs is None:
      vs_index = len(encoded_key) - (VS_SIZE + self._INDEX_SIZE)
      encoded_key += struct.pack('<L', vs_index)

  def decode(self, kvs):
    path_slice = slice(self.prefix_size, -1 * (VS_SIZE + self._INDEX_SIZE))
    vs_slice = slice(path_slice.stop, -1 * self._INDEX_SIZE)
    path = fdb.tuple.unpack(kvs[0].key[path_slice])
    commit_vs = kvs[0].key[vs_slice]
    first_index = ord(kvs[0].key[vs_slice.stop:])

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


class GroupUpdatesNS(object):
  DIR_NAME = u'group-updates'

  def __init__(self, directory):
    self.directory = directory

  @classmethod
  def from_cache(cls, project_id, namespace, directory_cache):
    directory = directory_cache.get((project_id, cls.DIR_NAME, namespace))
    return cls(directory)

  def encode(self, path):
    if not isinstance(path, tuple):
      path = encode_path(path)

    group_path = path[:2]
    val = b'\x00' * VS_SIZE + struct.pack('<L', 0)
    return self.encode_key(group_path), val

  def encode_key(self, group_path):
    return b''.join([self.directory.rawPrefix, hash_tuple(group_path),
                     fdb.tuple.pack(group_path)])


class DataManager(object):
  def __init__(self, directory_cache, tornado_fdb):
    self._directory_cache = directory_cache
    self._tornado_fdb = tornado_fdb

  @gen.coroutine
  def get_latest(self, tr, key, read_vs=None, include_data=True):
    data_ns = DataNamespace.from_key(key, self._directory_cache)
    desired_slice = data_ns.get_slice(key.path(), read_vs=read_vs)
    last_entry = yield self._last_version(
      tr, data_ns, desired_slice, include_data)
    if last_entry is None:
      last_entry = VersionEntry(data_ns.project_id, data_ns.namespace,
                                encode_path(key.path()))

    raise gen.Return(last_entry)

  @gen.coroutine
  def get_entry(self, tr, index_entry, snapshot=False):
    version_entry = yield self.get_version_from_path(
      tr, index_entry.project_id, index_entry.namespace, index_entry.path,
      index_entry.commit_vs, snapshot)
    raise gen.Return(version_entry)

  @gen.coroutine
  def get_version_from_path(self, tr, project_id, namespace, path, commit_vs,
                            snapshot=False):
    data_ns = DataNamespace.from_cache(
      project_id, namespace, self._directory_cache)
    desired_slice = data_ns.get_slice(path, commit_vs)
    kvs = yield self._get_range(tr, desired_slice, snapshot)
    raise gen.Return(data_ns.decode(kvs))

  @gen.coroutine
  def last_group_vs(self, tr, project_id, namespace, group_path):
    group_ns = GroupUpdatesNS.from_cache(
      project_id, namespace, self._directory_cache)
    last_updated_vs = yield self._tornado_fdb.get(tr, group_ns.encode_key(group_path))
    if not last_updated_vs.present():
      return

    raise gen.Return(last_updated_vs.value)

  def put(self, tr, key, version, encoded_entity):
    data_ns = DataNamespace.from_key(key, self._directory_cache)
    for fdb_key, val in data_ns.encode(key.path(), encoded_entity, version):
      tr[fdb_key] = val

    group_ns = GroupUpdatesNS.from_cache(
      data_ns.project_id, data_ns.namespace, self._directory_cache)
    tr.set_versionstamped_value(*group_ns.encode(key.path()))

  def hard_delete(self, tr, key, commit_vs):
    """ Only the GC should use this. """
    data_ns = DataNamespace.from_key(key, self._directory_cache)
    del tr[data_ns.get_slice(key.path(), commit_vs)]

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
