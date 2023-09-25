import types
import sys


PY2 = sys.version_info[0] == 2
PY3 = sys.version_info[0] == 3


if PY3:
    string_types = str,
    integer_types = int,
    class_types = type,
    text_type = str
    binary_type = bytes

else:
    string_types = basestring,    # noqa
    integer_types = (int, long)  # noqa
    class_types = (type, types.ClassType)
    text_type = unicode  # noqa
    binary_type = str


def to_bytes(value):
    if isinstance(value, text_type):
        return value.encode('utf-8', errors='replace')
    return value


def to_str(value):
    if isinstance(value, binary_type):
        return value.decode('utf-8', errors='replace')
    return value
