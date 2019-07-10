import six

from appscale.datastore.fdb.codecs import Int64, Path, TERMINATOR, Text
from appscale.datastore.fdb.utils import fdb, hash_tuple

first_gt_or_equal = fdb.KeySelector.first_greater_or_equal


class SequentialIDsNS(object):
  DIR_NAME = u'sequential-ids'

  def __init__(self, directory):
    self.directory = directory

  @classmethod
  def directory_path(cls, project_id, namespace):
    return project_id, cls.DIR_NAME, namespace

  def encode(self, path_prefix, largest_allocated):
    encoded_prefix = self._encode_prefix(path_prefix)
    return encoded_prefix + Int64.encode(largest_allocated)

  def decode(self, kv):
    pos = len(self.directory.rawPrefix)
    pos += 1  # scatter byte

    terminator = six.int2byte(TERMINATOR)
    kind_marker = six.int2byte(Path.KIND_MARKER)
    name_marker = six.int2byte(Path.NAME_MARKER)
    while pos < len(kv.key):
      marker = kv.key[pos]
      pos += 1
      if marker == terminator:
        break

      if marker == kind_marker:
        kind, pos = Text.decode(kv.key, pos)
      elif marker == name_marker:
        elem_name, pos = Text.decode(kv.key, pos)
      else:
        elem_id, pos = Int64.decode(marker, kv.key, pos)

    marker = kv.key[pos]
    pos += 1
    largest_allocated, pos = Int64.decode(marker, kv.key, pos)
    return largest_allocated

  def get_slice(self, path_prefix):
    encoded_prefix = self._encode_prefix(path_prefix)
    return slice(first_gt_or_equal(encoded_prefix),
                 first_gt_or_equal(encoded_prefix + b'\xFF'))

  def _encode_prefix(self, path_prefix):
    scatter_byte = hash_tuple(path_prefix)
    encoded_items = [self.directory.rawPrefix, scatter_byte]
    kind_marker = six.int2byte(Path.KIND_MARKER)
    for index in range(0, len(path_prefix[:-1]), 2):
      kind = path_prefix[index]
      id_or_name = path_prefix[index + 1]
      encoded_items.append(Text.encode(kind, kind_marker))
      encoded_items.append(Path.encode_id_or_name(id_or_name))

    encoded_items.append(Text.encode(path_prefix[-1], kind_marker))
    encoded_items.append(six.int2byte(TERMINATOR))
    return b''.join(encoded_items)
