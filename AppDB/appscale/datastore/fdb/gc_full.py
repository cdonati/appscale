from __future__ import division

import logging
import monotonic
import time
import uuid

from fdb.directory_impl import DirectorySubspace


from tornado import gen



from appscale.datastore.dbconstants import MAX_TX_DURATION
from appscale.datastore.fdb.utils import (
  fdb, MAX_FDB_TX_DURATION, KVIterator)

logger = logging.getLogger(__name__)


@gen.coroutine
def list_subdirectories(self, tr, directory):
  subdirectories = []
  more_results = True
  iteration = 1
  while more_results:
    kvs, count, more_results = yield self.get_range(
      tr, subdirs_subspace(directory).range(), iteration=iteration)
    subdirectories.extend([kv_to_dir(directory, kv) for kv in kvs])
    iteration += 1

  raise gen.Return(subdirectories)

def subdirs_subspace(directory):
  """ Returns the subspace that the directory layer uses to keep track of
      child directories.

  Args:
    directory: The parent DirectorySubspace object.

  Returns:
    A Subspace.
  """
  dir_layer = directory._directory_layer
  parent_subspace = dir_layer._node_with_prefix(directory.rawPrefix)
  return parent_subspace.subspace((dir_layer.SUBDIRS,))


def kv_to_dir(parent, kv):
  name = subdirs_subspace(parent).unpack(kv.key)[0]
  path = parent.get_path() + (name,)
  return DirectorySubspace(path, kv.value)


class GarbageCollector(object):
  _DELETED_DIR = ('garbage', 'deleted_versions')

  LOCK_KEY = '_gc_lock'

  def __init__(self, db, tornado_fdb, lock, directory_cache):
    self._db = db
    self._tornado_fdb = tornado_fdb
    self._lock = lock
    self._directory_cache = directory_cache

  def start(self):
    IOLoop.current().spawn_callback(self._run_under_lock)

  @gen.coroutine
  def clear_version(self, project_id, version_prefixes, gc_keys, tr=None):
    create_transaction = tr is None
    if create_transaction:
      tr = self._db.create_transaction()

    for version_prefix in version_prefixes:
      version_range = fdb.Subspace(rawPrefix=version_prefix).range()
      del tr[version_range]

    for gc_key in gc_keys:
      del tr[gc_key]

    if create_transaction:
      yield self._tornado_fdb.commit(tr)

  @gen.coroutine
  def _run_under_lock(self):
    while True:
      try:
        yield self._lock.acquire()
        yield self._clean_garbage()
      except Exception:
        logger.exception('Unable to clean garbage')
        yield gen.sleep(10)

  @gen.coroutine
  def _get_disposable_ranges(self):
    """ Fetches key ranges that will be disposable after a safe time period.

    Returns:
      A list of (namespace, range) tuples that can be cleared later.
    """
    disposable_ranges = []
    ds_dir = self._directory_cache.root
    tr = self._db.create_transaction()
    project_dirs = yield self._tornado_fdb.list_subdirectories(tr, ds_dir)
    for project_dir in project_dirs:
      namespace_dirs = yield self._tornado_fdb.list_subdirectories(
        tr, project_dir)
      for namespace_dir in namespace_dirs:
        namespace = namespace_dir.get_path()[-2:]
        gc_dir = self._directory_cache.get(namespace + Directories.DELETED)
        kvs, count, more_results = yield self._tornado_fdb.get_range(
          tr, gc_dir.range(), limit=1, reverse=True, snapshot=True)
        if not count:
          continue

        disposable_range = slice(
          gc_dir.range().start, fdb.KeySelector.first_greater_than(kvs[0].key))
        disposable_ranges.append((namespace, disposable_range))

    raise gen.Return(disposable_ranges)

  @gen.coroutine
  def _clear_ranges(self, disposable_ranges):
    versions_deleted = 0
    for namespace, disposable_range in disposable_ranges:
      gc_dir = self._directory_cache.get(namespace + Directories.DELETED)
      work_cutoff = monotonic.monotonic() + MAX_FDB_TX_DURATION / 2
      tr = self._db.create_transaction()
      iterator = KVIterator(self._tornado_fdb, tr, disposable_range)
      while True:
        kv = yield iterator.next()
        if kv is None:
          break

        path_with_version = fdb.tuple.unpack(kv.value)
        path = path_with_version[:-1]
        version = path_with_version[-1]
        gc_versionstamp, op_id = gc_dir.unpack(kv.key)

        yield self.clear_version(namespace, path, version, gc_versionstamp,
                                 op_id, tr)
        versions_deleted += 1
        if monotonic.monotonic() > work_cutoff:
          break

      yield self._tornado_fdb.commit(tr)
      if versions_deleted:
        logger.info('Removed {} old entity versions'.format(versions_deleted))

  @gen.coroutine
  def _clean_garbage(self):
    disposable_ranges = yield self._get_disposable_ranges()
    if not disposable_ranges:
      yield gen.sleep(MAX_TX_DURATION)
      return

    # Wait until any existing transactions to expire before removing the
    # deleted versions.
    yield gen.sleep(MAX_TX_DURATION)
    if not self._lock.acquired:
      return

    yield self._clear_ranges(disposable_ranges)
