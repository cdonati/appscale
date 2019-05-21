"""
This module stores and retrieves entity data as well as the metadata needed to
achieve snapshot isolation during transactions. The DataManager is the main
interface that clients can use to interact with the data layer. See its
documentation for implementation details.
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
from appscale.datastore.fdb.cache import DirectoryCache
from appscale.datastore.fdb.codecs import (
  decode_path, decode_sortable_int, decode_str, encode_path,
  encode_sortable_int, encode_vs_index)
from appscale.datastore.fdb.utils import (
  ABSENT_VERSION, DS_ROOT, EncodedTypes, fdb, hash_tuple, KVIterator,
  MAX_ENTITY_SIZE, VS_SIZE)

sys.path.append(APPSCALE_PYTHON_APPSERVER)
from google.appengine.datastore import entity_pb

logger = logging.getLogger(__name__)


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


class DataNSCache(DirectoryCache):
  # The number of items the cache can hold.
  SIZE = 512

  def __init__(self, tornado_fdb, project_cache):
    super(DataNSCache, self).__init__(tornado_fdb, project_cache.root_dir,
                                      self.SIZE)
    self._project_cache = project_cache

  @gen.coroutine
  def get(self, tr, project_id, namespace):
    yield self.validate_cache(tr)
    key = (project_id, namespace)
    if key not in self:
      project_dir = yield self._project_cache.get(project_id)
      # TODO: Make async.
      ns_dir = project_dir.create_or_open(
        tr, (DataNamespace.DIR_NAME, namespace))
      self[key] = DataNamespace(ns_dir)

    raise gen.Return(self[key])

  @gen.coroutine
  def get_from_key(self, tr, key):
    project_id = decode_str(key.app())
    namespace = decode_str(key.name_space())
    yield self.get(tr, project_id, namespace)


class DataNamespace(object):
  """
  The DataNamespace handles the encoding and decoding details for entity data
  for a specific project_id/namespace combination.

  The directory path looks like (<project-dir>, 'data', <namespace>).

  Within this directory, keys are encoded as
  <scatter-byte> + <path-tuple> + <commit-vs> + <index>.

  The <scatter-byte> is a single byte determined by hashing the entity path.
  Its purpose is to spread writes more evenly across the cluster and minimize
  hotspots.

  The <path-tuple> is an encoded tuple containing the entity path.

  The <commit-vs> is a 10-byte versionstamp that specifies the commit version
  of the transaction that wrote the entity data.

  The <index> is a single byte specifying which chunk number the KV contains.

  Values are encoded as <entity-version> + <entity-encoding> + <entity>.

  The <entity-version> is an integer specifying the approximate insert
  timestamp in microseconds (according to the client performing the insert).
  Though there is a one-to-one mapping of commit versionstamps to entity
  versions, the datastore uses a different value for the entity version in
  order to satisfy the 8-byte constraint and to follow the GAE convention of
  the value representing a timestamp. It is encoded using 7 bytes.

  The <entity-encoding> is a single byte specifying the encoding scheme of the
  entity to follow.

  The <entity> is an encoded protobuffer value.

  Since encoded values can exceed the size limit imposed by FoundationDB,
  values encoded values are split into chunks. Each chunk is stored as a
  KV and ordered by a unique <index> byte.
  """
  DIR_NAME = u'data'

  # The max number of bytes for each FDB value.
  _CHUNK_SIZE = 10000

  # The number of bytes used to store an entity version.
  _VERSION_SIZE = 7

  # The number of bytes used to encode the chunk index.
  _INDEX_SIZE = 1

  def __init__(self, directory):
    self.directory = directory

  @property
  def project_id(self):
    return self.directory.get_path()[len(DS_ROOT)]

  @property
  def namespace(self):
    return self.directory.get_path()[len(DS_ROOT) + 2]

  @property
  def path_slice(self):
    """ The portion of keys that contain the encoded path. """
    return slice(len(self.directory.rawPrefix) + 1,
                 -1 * (VS_SIZE + self._INDEX_SIZE))

  @property
  def vs_slice(self):
    """ The portion of keys that contain the commit versionstamp. """
    return slice(self.path_slice.stop, self.path_slice.stop + VS_SIZE)

  @property
  def index_slice(self):
    """ The portion of keys that contain the chunk index. """
    return slice(-1 * self._INDEX_SIZE, None)

  @property
  def version_slice(self):
    """ The portion of values that contain the entity version. """
    return slice(None, self._VERSION_SIZE)

  @property
  def encoding_slice(self):
    """ The portion of values that specify the entity encoding type. """
    return slice(self._VERSION_SIZE, self._VERSION_SIZE + 1)

  @property
  def entity_slice(self):
    """ The portion of values that contain the encoded entity. """
    return slice(self._VERSION_SIZE + 1, None)

  def get_slice(self, path, commit_vs=None, read_vs=None):
    """ Gets the range of keys relevant to the given constraints.

    Args:
      path: A tuple or protobuf path object.
      commit_vs: The commit versionstamp for a specific entity version.
      read_vs: The transaction's read versionstamp. All newer entity versions
        are ignored.
    Returns:
      A slice specifying the start and stop keys.
    """
    path_prefix = self._encode_path_prefix(path)
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

  def encode(self, path, entity, version):
    """ Encodes a tuple of KV tuples for a given version entry.

    Args:
      path: A tuple or protobuf path object.
      entity: An encoded entity or protobuf object.
      version: An integer specifying the new entity version.
    Returns:
      A tuple of KV tuples suitable for writing in an FDB transaction.
    """
    if isinstance(entity, entity_pb.EntityProto):
      entity = entity.Encode()

    if len(entity) > MAX_ENTITY_SIZE:
      raise BadRequest(u'Entity exceeds maximum size')

    encoded_version = encode_sortable_int(version, self._VERSION_SIZE)
    full_value = b''.join([encoded_version, EncodedTypes.ENTITY_V3, entity])
    chunk_count = int(math.ceil(len(full_value) / self._CHUNK_SIZE))
    return tuple(self._encode_kv(full_value, index, path, commit_vs=None)
                 for index in sm.range(chunk_count))

  def encode_key(self, path, commit_vs, index):
    """ Encodes a key for the given version entry.

    Args:
      path: A tuple or protobuf path object.
      commit_vs: A 10-byte string specifying the version's commit versionstamp.
      index: An integer specifying the chunk index.
    Returns:
      A string containing an FDB key. If commit_vs was none, the key should be
      used with set_versionstamped_key.
    """
    encoded_vs = b'\x00' * VS_SIZE if commit_vs is None else commit_vs
    encoded_index = bytes(bytearray((index,)))
    encoded_key = self._encode_path_prefix(path) + encoded_vs + encoded_index
    if commit_vs is None:
      vs_index = len(encoded_key) - (VS_SIZE + self._INDEX_SIZE)
      encoded_key += encode_vs_index(vs_index)

    return encoded_key

  def decode(self, kvs):
    path = fdb.tuple.unpack(kvs[0].key[self.path_slice])
    commit_vs = kvs[0].key[self.vs_slice]
    first_index = ord(kvs[0].key[self.index_slice])

    encoded_entity = None
    version = None
    if first_index == 0:
      encoded_val = b''.join([kv.value for kv in kvs])
      version = decode_sortable_int(encoded_val[self.version_slice])
      encoding = encoded_val[self.encoding_slice]
      encoded_entity = encoded_val[self.entity_slice]
      if encoding != EncodedTypes.ENTITY_V3:
        raise InternalError(u'Unknown entity type')

    return VersionEntry(self.project_id, self.namespace, path, commit_vs,
                        encoded_entity, version)

  def _encode_path_prefix(self, path):
    if not isinstance(path, tuple):
      path = encode_path(path)

    return b''.join([self.directory.rawPrefix, hash_tuple(path),
                     fdb.tuple.pack(path)])

  def _encode_kv(self, full_value, index, path, commit_vs):
    data_range = slice(index * self._CHUNK_SIZE,
                       (index + 1) * self._CHUNK_SIZE)
    encoded_val = full_value[data_range]
    return self.encode_key(path, commit_vs, index), encoded_val


class GroupUpdatesNSCache(DirectoryCache):
  """ Caches GroupUpdatesNS objects to keep track of FDB directory prefixes. """

  # The number of items the cache can hold.
  SIZE = 512

  def __init__(self, tornado_fdb, project_cache):
    super(GroupUpdatesNSCache, self).__init__(
      tornado_fdb, project_cache.root_dir, self.SIZE)
    self._project_cache = project_cache

  @gen.coroutine
  def get(self, tr, project_id, namespace):
    """ Gets a GroupUpdatesNS for a given project and namespace.

    Args:
      tr: An FDB transaction.
      project_id: A string specifying the project ID.
      namespace: A string specifying the namespace.
    """
    yield self.validate_cache(tr)
    key = (project_id, namespace)
    if key not in self:
      project_dir = yield self._project_cache.get(project_id)
      # TODO: Make async.
      ns_dir = project_dir.create_or_open(
        tr, (GroupUpdatesNS.DIR_NAME, namespace))
      self[key] = GroupUpdatesNS(ns_dir)

    raise gen.Return(self[key])


class GroupUpdatesNS(object):
  """
  The GroupUpdatesNS handles the encoding and decoding details for commit
  versionstamps for each entity group. These are used to materialize conflicts
  for transactions that involve ancestory queries on the same entity groups.

  The directory path looks like (<project-dir>, 'group-updates', <namespace>).

  Within this directory, keys are encoded as <scatter-byte> + <path-tuple>.

  The <scatter-byte> is a single byte determined by hashing the group path.
  Its purpose is to spread writes more evenly across the cluster and minimize
  hotspots.

  The <path-tuple> is an encoded tuple containing the group path.

  Values are 10-byte strings that specify the latest commit version for the
  entity group.
  """
  DIR_NAME = u'group-updates'

  def __init__(self, directory):
    self.directory = directory

  def encode(self, path):
    """ Creates a KV tuple for updating a group's commit versionstamp.

    Args:
      path: A tuple or protobuf path object.
    Returns:
      A (key, value) tuple suitable for tr.set_versionstamped_value.
    """
    if not isinstance(path, tuple):
      path = encode_path(path)

    group_path = path[:2]
    val = b'\x00' * VS_SIZE + struct.pack('<L', 0)
    return self.encode_key(group_path), val

  def encode_key(self, group_path):
    """ Encodes a key for a given entity group.

    Args:
      group_path: A tuple containing path elements.
    Returns:
      A byte string containing the relevant FDB key.
    """
    return b''.join([self.directory.rawPrefix, hash_tuple(group_path),
                     fdb.tuple.pack(group_path)])


class DataManager(object):
  """
  The DataManager is the main interface that clients can use to interact with
  the data layer. It makes use of the DataNamespace and GroupUpdateNS
  namespaces to handle the encoding and decoding details when satisfying
  requests. When a client requests data, the DataManager encapsulates entity
  data in a VersionEntry object.

  See the DataNamespace and GroupUpdateNS classes for implementation details
  about how data is stored and retrieved.
  """
  def __init__(self, tornado_fdb, project_cache):
    self._tornado_fdb = tornado_fdb
    self._data_cache = DataNSCache(self._tornado_fdb, project_cache)
    self._group_updates_cache = GroupUpdatesNSCache(
      self._tornado_fdb, project_cache)

  @gen.coroutine
  def get_latest(self, tr, key, read_vs=None, include_data=True):
    """ Gets the newest entity version for the given read VS.

    Args:
      tr: An FDB transaction.
      key: A protubuf reference object.
      read_vs: A 10-byte string specifying the FDB read versionstamp. Newer
        versionstamps are ignored.
      include_data: A boolean specifying whether or not to fetch all of the
        entity's KVs.
    """
    data_ns = yield self._data_cache.get_from_key(tr, key)
    desired_slice = data_ns.get_slice(key.path(), read_vs=read_vs)
    last_entry = yield self._last_version(
      tr, data_ns, desired_slice, include_data)
    if last_entry is None:
      last_entry = VersionEntry(data_ns.project_id, data_ns.namespace,
                                encode_path(key.path()))

    raise gen.Return(last_entry)

  @gen.coroutine
  def get_entry(self, tr, index_entry, snapshot=False):
    """ Gets the entity data from an index entry.

    Args:
      tr: An FDB transaction.
      index_entry: An IndexEntry object.
      snapshot: If True, the read will not cause a transaction conflict.
    Returns:
      A VersionEntry or None.
    """
    version_entry = yield self.get_version_from_path(
      tr, index_entry.project_id, index_entry.namespace, index_entry.path,
      index_entry.commit_vs, snapshot)
    raise gen.Return(version_entry)

  @gen.coroutine
  def get_version_from_path(self, tr, project_id, namespace, path, commit_vs,
                            snapshot=False):
    """ Gets the entity data for a specific version.

    Args:
      tr: An FDB transaction.
      project_id: A string specifying the project ID.
      namespace: A string specifying the namespace.
      path: A tuple or protobuf path object.
      commit_vs: A 10-byte string specyfing the FDB commit versionstamp.
      snapshot: If True, the read will not cause a transaction conflict.
    Returns:
      A VersionEntry or None.
    """
    data_ns = yield self._data_cache.get(tr, project_id, namespace)
    desired_slice = data_ns.get_slice(path, commit_vs)
    kvs = yield self._get_range(tr, desired_slice, snapshot)
    raise gen.Return(data_ns.decode(kvs))

  @gen.coroutine
  def last_group_vs(self, tr, project_id, namespace, group_path):
    """ Gets the most recent commit versionstamp for the entity group.

    Args:
      tr: An FDB transaction.
      project_id: A string specifying the project ID.
      namespace: A string specifying the namespace.
      group_path: A tuple containing the group's path elements.
    Returns:
      A 10-byte string specifying the versionstamp or None.
    """
    group_ns = yield self._group_updates_cache.get(tr, project_id, namespace)
    last_updated_vs = yield self._tornado_fdb.get(
      tr, group_ns.encode_key(group_path))
    if not last_updated_vs.present():
      return

    raise gen.Return(last_updated_vs.value)

  def put(self, tr, key, version, encoded_entity):
    """ Writes a new version entry and updates the entity group VS.

    Args:
      tr: An FDB transaction.
      key: A protobuf reference object.
      version: An integer specifying the new entity version.
      encoded_entity: A string specifying the encoded entity data.
    """
    data_ns = yield self._data_cache.get_from_key(tr, key)
    for fdb_key, val in data_ns.encode(key.path(), encoded_entity, version):
      tr[fdb_key] = val

    group_ns = yield self._group_updates_cache.get(
      tr, data_ns.project_id, data_ns.namespace)
    tr.set_versionstamped_value(*group_ns.encode(key.path()))

  def hard_delete(self, tr, key, commit_vs):
    """ Deletes a version entry. Only the GC should use this.

    Args:
      tr: An FDB transaction.
      key: A protobuf reference object.
      commit_vs: A 10-byte string specifying the commit versionstamp.
    """
    data_ns = yield self._data_cache.get_from_key(tr, key)
    del tr[data_ns.get_slice(key.path(), commit_vs)]

  @gen.coroutine
  def _last_version(self, tr, data_ns, desired_slice, include_data=True):
    """ Gets the most recent entity data for a given slice.

    Args:
      tr: An FDB transaction.
      data_ns: A DataNamespace.
      desired_slice: A slice specifying the start and stop keys.
      include_data: A boolean indicating that all chunks should be fetched.
    Returns:
      A VersionEntry or None.
    """
    kvs, count, more_results = yield self._tornado_fdb.get_range(
      tr, desired_slice, limit=1, reverse=True)

    if not kvs:
      return

    last_kv = kvs[0]
    entry = data_ns.decode([last_kv])
    if not include_data or entry.complete:
      raise gen.Return(entry)

    # Retrieve the remaining chunks.
    version_slice = data_ns.get_slice(entry.path, entry.commit_vs)
    end_key = data_ns.encode_key(entry.path, entry.commit_vs, entry.index)
    remaining_slice = slice(version_slice.start,
                            fdb.KeySelector.first_greater_or_equal(end_key))
    kvs = self._get_range(tr, remaining_slice)
    raise gen.Return(data_ns.decode(kvs + [last_kv]))

  @gen.coroutine
  def _get_range(self, tr, data_range, snapshot=False):
    """ Gets a list of KVs for a given slice.

    Args:
      tr: An FDB transaction.
      data_range: A slice specifying the start and stop keys.
      snapshot: If True, the read will not cause a transaction conflict.
    Returns:
      A list of KVs in the given range.
    """
    iterator = KVIterator(tr, self._tornado_fdb, data_range, snapshot=snapshot)
    all_kvs = []
    while True:
      kvs, more_results = yield iterator.next_page()
      all_kvs.extend(kvs)
      if not more_results:
        break

    raise gen.Return(all_kvs)
