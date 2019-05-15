from tornado import gen

from appscale.datastore.dbconstants import InternalError


class DirectoryCache(object):
  # The root directory that the datastore uses.
  ROOT = ('appscale', 'datastore')

  # The number of items the cache can hold.
  SIZE = 1024

  # The location of the metadata version key.
  METADATA_KEY = b'\xff/metadataVersion'

  def __init__(self, tornado_fdb):
    self._directory_dict = {}
    self._directory_list = []
    self._metadata_version = None
    self._tornado_fdb = tornado_fdb

  @gen.coroutine
  def get(self, tr, path):
    yield self._ensure_valid(tr)

  @gen.coroutine
  def _ensure_valid(self, tr):
    current_version = yield self._tornado_fdb.get(tr, self.METADATA_KEY)
    if not current_version.present():
      raise InternalError(u'The FDB cluster metadata key is missing')

    if current_version.value != self._metadata_version:
      self._metadata_version = current_version.value
      self._directory_dict = {}
      self._directory_list = []
