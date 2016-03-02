import datetime
import unittest
import uuid

import arrow

from sprockets.clients.dynamodb import utils


class UTC(datetime.tzinfo):
    def utcoffset(self, dt):
        return datetime.timedelta(0)

    def tzname(self, dt):
        return 'UTC'

    def dst(self, dt):
        return datetime.timedelta(0)


class MarshallTests(unittest.TestCase):
    maxDiff = None

    def test_complex_document(self):
        uuid_value = uuid.uuid4()
        arrow_value = arrow.utcnow()
        dt_value = datetime.datetime.utcnow().replace(tzinfo=UTC())
        value = {
            'key1': 'str',
            'key2': 10,
            'key3': {
                'sub-key1': 20,
                'sub-key2': True,
                'sub-key3': 'value'
            },
            'key4': None,
            'key5': ['one', 'two', 'three', 4, None, True],
            'key6': set(['a', 'b', 'c']),
            'key7': {1, 2, 3, 4},
            'key8': arrow_value,
            'key9': uuid_value,
            'key10': b'\0x01\0x02\0x03',
            'key11': {b'\0x01\0x02\0x03', b'\0x04\0x05\0x06'},
            'key12': dt_value
        }
        expectation = {
            'key1': {'S': 'str'},
            'key2': {'N': '10'},
            'key3': {'M':
                {
                    'sub-key1': {'N': '20'},
                    'sub-key2': {'BOOL': True},
                    'sub-key3': {'S': 'value'}
                }
            },
            'key4': {'NULL': True},
            'key5': {'L': [{'S': 'one'}, {'S': 'two'}, {'S': 'three'},
                           {'N': '4'}, {'NULL': True}, {'BOOL': True}]},
            'key6': {'SS': ['a', 'b', 'c']},
            'key7': {'NS': ['1', '2', '3', '4']},
            'key8': {'S': arrow_value.isoformat()},
            'key9': {'S': str(uuid_value)},
            'key10': {'B': b'\0x01\0x02\0x03'},
            'key11': {'BS': [b'\0x01\0x02\0x03', b'\0x04\0x05\0x06']},
            'key12': {'S': dt_value.isoformat()}
        }
        self.assertDictEqual(expectation, utils.marshall(value))

    def test_value_error_raised_on_unsupported_type(self):
        self.assertRaises(ValueError, utils.marshall, {'key': self})

    def test_value_error_raised_on_mixed_set(self):
        self.assertRaises(ValueError, utils.marshall, {'key': {1, 'two', 3}})


class UnmarshallTests(unittest.TestCase):
    maxDiff = None

    def test_complex_document(self):
        uuid_value = uuid.uuid4()
        dt_value = arrow.utcnow()
        value = {
            'key1': {'S': 'str'},
            'key2': {'N': '10'},
            'key3': {'M':
                {
                    'sub-key1': {'N': '20'},
                    'sub-key2': {'BOOL': True},
                    'sub-key3': {'S': 'value'}
                }
            },
            'key4': {'NULL': True},
            'key5': {'L': [{'S': 'one'}, {'S': 'two'}, {'S': 'three'},
                           {'N': '4'}, {'NULL': True}, {'BOOL': True}]},
            'key6': {'SS': ['a', 'b', 'c']},
            'key7': {'NS': ['1', '2', '3', '4']},
            'key8': {'S': dt_value.isoformat()},
            'key9': {'S': str(uuid_value)},
            'key10': {'B': b'\0x01\0x02\0x03'},
            'key11': {'BS': [b'\0x01\0x02\0x03', b'\0x04\0x05\0x06']}
        }
        expectation = {
            'key1': 'str',
            'key2': 10,
            'key3': {
                'sub-key1': 20,
                'sub-key2': True,
                'sub-key3': 'value'
            },
            'key4': None,
            'key5': ['one', 'two', 'three', 4, None, True],
            'key6': {'a', 'b', 'c'},
            'key7': {1, 2, 3, 4},
            'key8': dt_value.isoformat(),
            'key9': uuid_value,
            'key10': b'\0x01\0x02\0x03',
            'key11': {b'\0x01\0x02\0x03', b'\0x04\0x05\0x06'}
        }
        self.assertDictEqual(expectation, utils.unmarshall(value))

    def test_value_error_raised_on_unsupported_type(self):
        self.assertRaises(ValueError, utils.unmarshall, {'key': {'T': 1}})
