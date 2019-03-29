import sys

import six

from appscale.common.unpackaged import APPSCALE_PYTHON_APPSERVER
from appscale.datastore.dbconstants import BadRequest

sys.path.append(APPSCALE_PYTHON_APPSERVER)
from google.appengine.datastore import entity_pb


class V3Types(object):
  NULL = b'\x00'
  INT64 = b'\x01'
  BOOLEAN = b'\x02'
  STRING = b'\x03'
  DOUBLE = b'\x04'
  POINT = b'\x05'
  USER = b'\x06'
  REFERENCE = b'\x07'


def encode_element(element):
  if element.has_id():
    id_or_name = element.id()
  elif element.has_name():
    id_or_name = six.text_type(element.name())
  else:
    raise BadRequest('All path elements must either have a name or ID')

  return six.text_type(element.type()), id_or_name


def encode_path(path):
  if isinstance(path, entity_pb.PropertyValue_ReferenceValue):
    element_list = path.pathelement_list()
  else:
    element_list = path.element_list()

  return tuple(encode_element(element) for element in element_list)


def decode_path(flattened_path, reference_value=False):
  if reference_value:
    path = entity_pb.PropertyValue_ReferenceValue()
  else:
    path = entity_pb.Path()

  for index in range(0, len(flattened_path), 2):
    element = path.add_element()
    element.set_type(flattened_path[index])
    id_or_name = flattened_path[index + 1]
    if isinstance(id_or_name, int):
      element.set_id(id_or_name)
    else:
      element.set_name(id_or_name)

  return path


def reverse_encode_string(unicode_string):
  byte_array = bytearray(unicode_string, encoding='utf-8')
  for index, byte_value in enumerate(byte_array):
    byte_array[index] = 255 - (byte_value + 1)

  return bytes(byte_array) + b'\xff'


def encode_reference(val):
  project_id = six.text_type(val.app())
  namespace = six.text_type(val.name_space())
  return (project_id, namespace) + flat_path(val)


def decode_point(val):
  point_val = entity_pb.PropertyValue_PointValue()
  point_val.set_x(val[0])
  point_val.set_y(val[1])
  return point_val


def decode_user(val):
  user_val = entity_pb.PropertyValue_UserValue()
  user_val.set_email(val[0])
  user_val.set_auth_domain(val[1])
  return user_val


def decode_reference(val):
  reference_val = entity_pb.PropertyValue_ReferenceValue()
  reference_val.set_app(val[0])
  reference_val.set_name_space(val[1])
  reference_val.MergeFrom(
    decode_path(value_parts[2:], reference_value=True))
  decode_path(value_parts[2:], reference_value=True)
  for kind, id_or_name in val[2:]:
    reference_val.add_pathelement()


def reverse_type(encoded_type):
  return bytes(bytearray([255 - ord(encoded_type)]))


ENCODERS = {
  V3Types.NULL: lambda val: tuple(),
  V3Types.INT64: lambda val: (val,),
  V3Types.BOOLEAN: lambda val: (val,),
  V3Types.STRING: lambda val: (six.text_type(val),),
  V3Types.DOUBLE: lambda val: (val,),
  V3Types.POINT: lambda val: (val.x(), val.y()),
  V3Types.USER: lambda val: (six.text_type(val.email()),
                             six.text_type(val.auth_domain())),
  V3Types.REFERENCE: encode_reference,
  reverse_type(V3Types.NULL): lambda val: tuple(),
  reverse_type(V3Types.INT64): lambda val: (val * -1,),
  reverse_type(V3Types.BOOLEAN): lambda val: (not val,),
  reverse_type(V3Types.STRING):
    lambda val: (reverse_encode_string(six.text_type(val)),),
  reverse_type(V3Types.DOUBLE): lambda val: (val * -1,),
  reverse_type(V3Types.POINT): lambda val: (val.x() * -1, val.y() * -1),
  reverse_type(V3Types.USER):
    lambda val: (reverse_encode_string(six.text_type(val.email())),
                 reverse_encode_string(six.text_type(val.auth_domain()))),
  reverse_type(V3Types.REFERENCE):
    lambda val: tuple(reverse_encode_string(item)
                      for item in encode_reference(val))
}


DECODERS = {
  V3Types.INT64: lambda val: val[0],
  V3Types.BOOLEAN: lambda val: val[0],
  V3Types.STRING: lambda val: val[0],
  V3Types.DOUBLE: lambda val: val[0],
  V3Types.POINT: decode_point,
  V3Types.USER: decode_user,
  V3Types.REFERENCE: encode_reference,
  reverse_type(V3Types.NULL): lambda val: tuple(),
  reverse_type(V3Types.INT64): lambda val: (val * -1,),
  reverse_type(V3Types.BOOLEAN): lambda val: (not val,),
  reverse_type(V3Types.STRING):
    lambda val: (reverse_encode_string(six.text_type(val)),),
  reverse_type(V3Types.DOUBLE): lambda val: (val * -1,),
  reverse_type(V3Types.POINT): lambda val: (val.x() * -1, val.y() * -1),
  reverse_type(V3Types.USER):
    lambda val: (reverse_encode_string(six.text_type(val.email())),
                 reverse_encode_string(six.text_type(val.auth_domain()))),
  reverse_type(V3Types.REFERENCE):
    lambda val: tuple(reverse_encode_string(item)
                      for item in encode_reference(val))
}


def unpack_value(value):
  readable_types = [
    (name.lower(), encoded) for name, encoded in V3Types.__dict__.items()
    if not name.startswith('_') and encoded != V3Types.NULL]
  for type_name, encoded_type in readable_types:
    if getattr(value, 'has_{}value'.format(type_name))():
      return encoded_type, getattr(value, '{}value'.format(type_name))()

  return V3Types.NULL, None


def encode_value(value, reverse=False):
  encoded_type, value = unpack_value(value)
  if reverse:
    encoded_type = reverse_type(encoded_type)

  return (encoded_type,) + ENCODERS[encoded_type](value)


def decode_value(unpacked_key):
  value = entity_pb.PropertyValue()
  encoded_type = unpacked_key[0]
  if encoded_type == V3Types.NULL:
    return value, unpacked_key[1:]
  type_name = next(key for key, val in V3Types.__dict__.items()
                   if val == encoded_type).lower()

  if encoded_type not in SIMPLE_TYPES:
    type_name = get_type_name(encoded_type)
    getattr(value, 'set_{}value'.format(type_name))(unpacked_key[1])
    return value, unpacked_key[2:]

  if encoded_type == V3Types.POINT:
    point_val = value.mutable_pointvalue()
    point_val.set_x(unpacked_key[1])
    point_val.set_y(unpacked_key[2])
    return value, unpacked_key[3:]

  if encoded_type == V3Types.USER:
    user_val = value.mutable_uservalue()
    user_val.set_email(unpacked_key[1])
    user_val.set_email(unpacked_key[2])
    return value, unpacked_key[3:]

  if encoded_type == V3Types.REFERENCE:
    delimiter_index = unpacked_key.index(REF_VAL_DELIMETER)
    value_parts = unpacked_key[1:delimiter_index]
    reference_val = value.mutable_referencevalue()
    reference_val.set_app(value_parts[0])
    reference_val.set_name_space(value_parts[1])
    reference_val.MergeFrom(
      decode_path(value_parts[2:], reference_value=True))
    return value, unpacked_key[slice(delimiter_index + 1, None)]

  raise InternalError(u'Unsupported PropertyValue type')