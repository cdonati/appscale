"""
This module allows FDB clients to cache directory mappings and invalidate
them when the schema changes.
"""
from collections import deque

from tornado import gen

from appscale.datastore.dbconstants import InternalError
from appscale.datastore.fdb.codecs import decode_str


class DirectoryCache(object):
  """ A simple directory cache that more specialized caches can extend. """

  # The location of the metadata version key.
  METADATA_KEY = b'\xff/metadataVersion'

  def __init__(self, tornado_fdb, root_dir, cache_size):
    self.root_dir = root_dir
    self._cache_size = cache_size
    self._directory_dict = {}
    self._directory_keys = deque()
    self._metadata_version = None
    self._tornado_fdb = tornado_fdb

  def __setitem__(self, key, value):
    if key not in self._directory_dict:
      self._directory_keys.append(key)
      if len(self._directory_keys) > self._cache_size:
        oldest_key = self._directory_keys.popleft()
        del self._directory_dict[oldest_key]

    self._directory_dict[key] = value

  def __getitem__(self, key):
    return self._directory_dict[key]

  def __contains__(self, item):
    return item in self._directory_dict

  @gen.coroutine
  def validate_cache(self, tr):
    """ Clears the cache if the metadata key has been updated. """
    current_version = yield self._tornado_fdb.get(tr, self.METADATA_KEY)
    if not current_version.present():
      raise InternalError(u'The FDB cluster metadata key is missing')

    if current_version.value != self._metadata_version:
      self._metadata_version = current_version.value
      self._directory_dict.clear()
      self._directory_keys.clear()


class ProjectCache(DirectoryCache):
  """ A directory cache that keeps track of projects. """

  # The number of items the cache can hold.
  SIZE = 256

  def __init__(self, tornado_fdb, root_dir):
    super(ProjectCache, self).__init__(tornado_fdb, root_dir, self.SIZE)

  @gen.coroutine
  def get(self, tr, project_id):
    """ Gets a project's directory.

    Args:
      tr: An FDB transaction.
      project_id: A string specifying a project ID.
    Returns:
      A DirectorySubspace object.
    """
    yield self.validate_cache(tr)
    if project_id not in self:
      # TODO: Check new projects instead of assuming they are valid.
      # This can also be made async.
      self[project_id] = self.root_dir.create_or_open(tr, (project_id,))

    raise gen.Return(self[project_id])


class NSCache(DirectoryCache):
  """ Caches namespace directories to keep track of directory prefixes. """

  # The number of items the cache can hold.
  SIZE = 512

  def __init__(self, tornado_fdb, project_cache, dir_type):
    super(NSCache, self).__init__(
      tornado_fdb, project_cache.root_dir, self.SIZE)
    self._project_cache = project_cache
    self._dir_type = dir_type

  @gen.coroutine
  def get(self, tr, project_id, namespace):
    """ Gets a namespace directory for the given project and namespace.

    Args:
      tr: An FDB transaction.
      project_id: A string specifying the project ID.
      namespace: A string specifying the namespace.
    Returns:
      A namespace directory object of the directory type.
    """
    yield self.validate_cache(tr)
    key = (project_id, namespace)
    if key not in self:
      project_dir = yield self._project_cache.get(project_id)
      # TODO: Make async.
      ns_dir = project_dir.create_or_open(
        tr, (self._dir_type.DIR_NAME, namespace))
      self[key] = self._dir_type(ns_dir)

    raise gen.Return(self[key])

  @gen.coroutine
  def get_from_key(self, tr, key):
    """ Gets a namespace directory for a protobuf reference object.

    Args:
      tr: An FDB transaction.
      key: A protobuf reference object.
    Returns:
      A namespace directory object of the directory type.
    """
    project_id = decode_str(key.app())
    namespace = decode_str(key.name_space())
    ns_dir = yield self.get(tr, project_id, namespace)
    raise gen.Return(ns_dir)
