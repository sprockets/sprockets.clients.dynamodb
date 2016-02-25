"""
Utilities for working with DynamoDB.

- :func:`.marshall`
- :func:`.unmarshal`

This module contains some helpers that make working with the
Amazon DynamoDB API a little less painful.  Data is encoded as
`AttributeValue`_ structures in the JSON payloads and this module
defines functions that will handle the transcoding for you for
the vast majority of types that we use.

.. _AttributeValue: http://docs.aws.amazon.com/amazondynamodb/latest/
   APIReference/API_AttributeValue.html

"""
import datetime
import uuid
import sys
try:
    import arrow
except ImportError:
    arrow = None


PYTHON3 = True if sys.version_info > (3, 0, 0) else False
TEXTCHARS = bytearray({7,8,9,10,12,13,27} | set(range(0x20, 0x100)) - {0x7f})


def marshall(values):
    """
    Marshall a `dict` into something DynamoDB likes.

    :param dict values: The values to marshall
    :rtype: dict
    :raises ValueError: if an unsupported type is encountered

    Return the values in a nested dict structure that is required for
    writing the values to DynamoDB.

    """
    serialized = {}
    for key in values:
        serialized[key] = _marshall_value(values[key])
    return serialized


def _marshall_value(value):
    """
    Recursively transform `value` into an AttributeValue `dict`

    :param mixed value: The value to encode
    :rtype: dict
    :raises ValueError: for unsupported types

    Return the value as dict indicating the data type and transform or
    recursively process the value if required.

    """
    if PYTHON3 and isinstance(value, bytes):
        return {'B': value}
    elif PYTHON3 and isinstance(value, str):
        return {'S': value}
    elif not PYTHON3 and isinstance(value, str):
        if _is_binary(value):
            return {'B': value}
        return {'S': value}
    elif isinstance(value, dict):
        return {'M': marshall(value)}
    elif isinstance(value, bool):
        return {'BOOL': value}
    elif isinstance(value, (int, float)):
        return {'N': str(value)}
    elif isinstance(value, datetime.datetime):
        return {'S': value.isoformat()}
    elif arrow is not None and isinstance(value, arrow.Arrow):
        return {'S': value.isoformat()}
    elif isinstance(value, uuid.UUID):
        return {'S': str(value)}
    elif isinstance(value, list):
        return {'L': [_marshall_value(v) for v in value]}
    elif isinstance(value, set):
        if PYTHON3 and all([isinstance(v, bytes) for v in value]):
            return {'BS': sorted(list(value))}
        elif PYTHON3 and all([isinstance(v, str) for v in value]):
            return {'SS': sorted(list(value))}
        elif all([isinstance(v, (int, float)) for v in value]):
            return {'NS': sorted([str(v) for v in value])}
        elif not PYTHON3 and all([isinstance(v, str) for v in value]) and \
                all([_is_binary(v) for v in value]):
            return {'BS': sorted(list(value))}
        elif not PYTHON3 and all([isinstance(v, str) for v in value]) and \
                all([_is_binary(v) is False for v in value]):
            return {'SS': sorted(list(value))}
        else:
            raise ValueError('Can not mix types in a set')
    elif value is None:
        return {'NULL': True}
    raise ValueError('Unsupported type: %s' % type(value))


def unmarshall(values):
    """
    Transform a response payload from DynamoDB to a native dict

    :param dict values: The response payload from DynamoDB
    :rtype: dict
    :raises ValueError: if an unsupported type code is encountered

    """
    unmarshalled = {}
    for key in values:
        unmarshalled[key] = _unmarshall_dict(values[key])
    return unmarshalled


def _unmarshall_dict(value):
    """Unmarshall a single dict value from a row that was returned from
    DynamoDB, returning the value as a normal Python dict.

    :param dict value: The value to unmarshall
    :rtype: mixed
    :raises ValueError: if an unsupported type code is encountered

    """
    key = list(value.keys()).pop()
    if key == 'B':
        return bytes(value[key])
    elif key == 'BS':
        return set([bytes(v) for v in value[key]])
    elif key == 'BOOL':
        return value[key]
    elif key == 'L':
        return [_unmarshall_dict(v) for v in value[key]]
    elif key == 'M':
        return unmarshall(value[key])
    elif key == 'NULL':
        return None
    elif key == 'N':
        return _to_number(value[key])
    elif key == 'NS':
        return set([_to_number(v) for v in value[key]])
    elif key == 'S':
        return _maybe_convert(value[key])
    elif key == 'SS':
        return set([_maybe_convert(v) for v in value[key]])
    raise ValueError('Unsupported value type: %s' % key)


def _to_number(value):
    """
    Convert the string containing a number to a number

    :param str value: The value to convert
    :rtype: float|int

    """
    return float(value) if '.' in value else int(value)


def _maybe_convert(value):
    """
    Try to convert a string into something useful.

    :param str value: The value to convert
    :rtype: uuid.UUID|datetime.datetime|str

    Possibly convert the value to a :py:class:`uuid.UUID` or
    :py:class:`datetime.datetime` if possible, otherwise just return
    the value.

    """
    try:
        return uuid.UUID(value)
    except ValueError:
        return value


def _is_binary(value):
    """
    Check to see if a string contains binary data in Python2

    :param str value: The value to check
    :rtype: bool

    """
    return bool(value.translate(None, TEXTCHARS))
