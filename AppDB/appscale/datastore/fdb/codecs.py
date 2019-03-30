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

  accessible = (
    ('int64', INT64), ('boolean', BOOLEAN), ('string', STRING),
    ('double', DOUBLE), ('point', POINT), ('user', USER),
    ('reference', REFERENCE))

  @classmethod
  def null(cls, encoded_type):
    return encoded_type == cls.NULL or cls.reverse(encoded_type) == cls.NULL

  @classmethod
  def scalar(cls, encoded_type):
    scalar_types = (cls.INT64, cls.BOOLEAN, cls.STRING, cls.DOUBLE)
    return (encoded_type in scalar_types or
            cls.reverse(encoded_type) in scalar_types)

  @classmethod
  def compound(cls, encoded_type):
    compound_types = (cls.POINT, cls.USER, cls.REFERENCE)
    return (encoded_type in compound_types or
            cls.reverse(encoded_type) in compound_types)

  @classmethod
  def name(cls, encoded_type):
    return next(key for key, val in V3Types.__dict__.items()
                if val == encoded_type or val == cls.reverse(encoded_type))

  @staticmethod
  def reverse(encoded_type):
    return bytes(bytearray([255 - ord(encoded_type)]))


def encode_element(element):
  if element.has_id():
    id_or_name = element.id()
  elif element.has_name():
    id_or_name = six.text_type(element.name())
  else:
    raise BadRequest('All path elements must either have a name or ID')

  return six.text_type(element.type()), id_or_name


def decode_element(element_tuple):
  path_element = entity_pb.Path_Element()
  path_element.set_type(element_tuple[0])
  if isinstance(element_tuple[1], int):
    path_element.set_id(element_tuple[1])
  else:
    path_element.set_name(element_tuple[1])

  return path_element


def encode_path(path):
  if isinstance(path, entity_pb.PropertyValue):
    element_list = path.referencevalue().pathelement_list()
  elif isinstance(path, entity_pb.PropertyValue_ReferenceValue):
    element_list = path.pathelement_list()
  else:
    element_list = path.element_list()

  return tuple(item for element in element_list
               for item in encode_element(element))


def decode_path(encoded_path, reference_value=False):
  if reference_value:
    path = entity_pb.PropertyValue_ReferenceValue()
  else:
    path = entity_pb.Path()

  for index in range(0, len(encoded_path), 2):
    element = path.add_element()
    element.set_type(encoded_path[index])
    id_or_name = encoded_path[index + 1]
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


def reverse_decode_string(byte_string):
  byte_array = bytearray(byte_string[:-1])
  for index, byte_value in enumerate(byte_array):
    byte_array[index] = (255 - byte_value) - 1

  return byte_array.decode('utf-8')


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


def encode_reference(val):
  project_id = six.text_type(val.app())
  namespace = six.text_type(val.name_space())
  return (project_id, namespace) + encode_path(val)


def decode_reference(val):
  reference_val = entity_pb.PropertyValue_ReferenceValue()
  reference_val.set_app(val[0])
  reference_val.set_name_space(val[1])
  reference_val.MergeFrom(decode_path(val[2:], reference_value=True))
  return reference_val


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
  V3Types.reverse(V3Types.NULL): lambda val: tuple(),
  V3Types.reverse(V3Types.INT64): lambda val: (val * -1,),
  V3Types.reverse(V3Types.BOOLEAN): lambda val: (not val,),
  V3Types.reverse(V3Types.STRING):
    lambda val: (reverse_encode_string(six.text_type(val)),),
  V3Types.reverse(V3Types.DOUBLE): lambda val: (val * -1,),
  V3Types.reverse(V3Types.POINT): lambda val: (val.x() * -1, val.y() * -1),
  V3Types.reverse(V3Types.USER):
    lambda val: (reverse_encode_string(six.text_type(val.email())),
                 reverse_encode_string(six.text_type(val.auth_domain()))),
  V3Types.reverse(V3Types.REFERENCE):
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
  V3Types.REFERENCE: decode_reference,
  V3Types.reverse(V3Types.INT64): lambda val: val[0] * -1,
  V3Types.reverse(V3Types.BOOLEAN): lambda val: not val[0],
  V3Types.reverse(V3Types.STRING): lambda val: reverse_decode_string(val[0]),
  V3Types.reverse(V3Types.DOUBLE): lambda val: val[0] * -1,
  V3Types.reverse(V3Types.POINT):
    lambda val: decode_point((val[0] * -1, val[1] * -1)),
  V3Types.reverse(V3Types.USER):
    lambda val: decode_user((reverse_decode_string(val[0]),
                             reverse_decode_string(val[1]))),
  V3Types.reverse(V3Types.REFERENCE):
    lambda val: decode_reference(tuple(reverse_decode_string(item)
                                       for item in val))
}


def unpack_value(value):
  for type_name, encoded_type in V3Types.accessible:
    if getattr(value, 'has_{}value'.format(type_name))():
      return encoded_type, getattr(value, '{}value'.format(type_name))()

  return V3Types.NULL, None


def encode_value(value, reverse=False):
  encoded_type, value = unpack_value(value)
  if reverse:
    encoded_type = V3Types.reverse(encoded_type)

  return (encoded_type,) + ENCODERS[encoded_type](value)


def decode_value(encoded_value):
  prop_value = entity_pb.PropertyValue()
  encoded_type = encoded_value[0]
  if V3Types.null(encoded_type):
    return prop_value

  decoded_value = DECODERS[encoded_type](encoded_value[1:])
  type_name = V3Types.name(encoded_type).lower()
  if V3Types.scalar(encoded_type):
    getattr(prop_value, 'set_{}value'.format(type_name))(decoded_value)
  elif V3Types.compound(encoded_type):
    compound_val = getattr(prop_value, 'mutable_{}value'.format(type_name))()
    compound_val.MergeFrom(decoded_value)

  return prop_value
