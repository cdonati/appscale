"""
data: This maps entity keys to encoded entity data. The data is a tuple that
specifies the version and the encoding. Due to FDB's value size limit, data
that exceeds the chunk size threshold is split into multiple key-values. The
index value indicates the position of the chunk. Here is an example template
along with an example key-value:

  ([directory^1], [path]^2, <vs^3>, <index>) -> (<version>, <encoding>, <data>)
  ([directory], Greeting, 476633..., <vs>, 0) -> (0, 155284..., <protobuffer>)
"""
import six
from tornado import gen

from appscale.datastore.dbconstants import InternalError
from appscale.datastore.fdb.utils import (
  ABSENT_VERSION, EncodedTypes, fdb, flat_path, put_chunks, RangeIterator)


class DataManager(object):
  DIRECTORY = u'data'

  def __init__(self, directory_cache, tornado_fdb):
    self._directory_cache = directory_cache
    self._tornado_fdb = tornado_fdb

  @gen.coroutine
  def get_latest(self, tr, key, read_vs=None):
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

    version, encoding, encoded_entity = fdb.tuple.unpack(''.join(chunks))
    if encoding != EncodedTypes.ENTITY_V3:
      raise InternalError('Unknown entity type')

    raise gen.Return((key, encoded_entity, version, commit_vs))

  def get_version(self, tr, key, commit_vs, snapshot):
    path_subspace = self._subspace_from_key(key)
    vs_subspace = path_subspace.subspace((commit_vs,))
    chunks = yield self._get_range(tr, vs_subspace.range(), snapshot)
    version, encoding, encoded_entity = fdb.tuple.unpack(''.join(chunks))
    if encoding != EncodedTypes.ENTITY_V3:
      raise InternalError('Unknown entity type')

    raise gen.Return((version, encoded_entity))

  @gen.coroutine
  def latest_vs(self, tr, key):
    path_subspace = self._subspace_from_key(key)
    _, commit_vs, _ = yield self._last_chunk(tr, path_subspace)
    raise gen.Return(commit_vs)

  def put(self, tr, key, version, encoded_entity):
    path_subspace = self._subspace_from_key(key)
    encoded_value = fdb.tuple.pack((version, EncodedTypes.ENTITY_V3,
                                    encoded_entity))
    put_chunks(tr, encoded_value, path_subspace, add_vs=True)

  def _subspace_from_key(self, key):
    project_id = six.text_type(key.app())
    namespace = six.text_type(key.name_space())
    path = flat_path(key)
    data_ns_dir = self._directory_cache.get(
      (project_id, self.DIRECTORY, namespace))
    return data_ns_dir.subspace(path)

  @gen.coroutine
  def _last_chunk(self, tr, path_subspace, read_vs=None):
    # Ignore values written after the start of the transaction.
    if read_vs is not None:
      vs_subspace = path_subspace.subspace((read_vs,))
      data_range = slice(path_subspace.start, vs_subspace.end)
    else:
      data_range = path_subspace.range()

    kvs, count, more_results = yield self._tornado_fdb.get_range(
      tr, data_range, limit=1, reverse=True)

    if not count:
      raise gen.Return((None, 0, None))

    chunk = kvs[0].value
    unpacked_key = path_subspace.unpack(kvs[0].key)
    commit_vs = unpacked_key[-2]
    data_index = unpacked_key[-1]
    raise gen.Return((chunk, commit_vs, data_index))

  @gen.coroutine
  def _get_range(self, tr, data_range, snapshot=False):
    iterator = RangeIterator(tr, self._tornado_fdb, data_range,
                             snapshot=snapshot)
    chunks = []
    while True:
      kvs, more_results = yield iterator.next_page()
      chunks.extend([kv.value for kv in kvs])
      if not more_results:
        break

    raise gen.Return(chunks)
