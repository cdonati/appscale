"""
This module allows FDB clients to cache directory mappings and invalidate
them when the schema changes.
"""

from collections import deque

from tornado import gen

from appscale.datastore.dbconstants import InternalError


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
