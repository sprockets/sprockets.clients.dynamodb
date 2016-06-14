import datetime
import os
import socket
import sys
import uuid
import unittest

import mock

from tornado import concurrent
from tornado import httpclient
from tornado import testing
from tornado_aws import exceptions as aws_exceptions

from sprockets.clients import dynamodb
from sprockets.clients.dynamodb import connector, exceptions


class AsyncTestCase(testing.AsyncTestCase):

    def setUp(self):
        super(AsyncTestCase, self).setUp()
        self.client = self.get_client()

    @property
    def endpoint(self):
        return os.getenv('DYNAMODB_ENDPOINT')

    @staticmethod
    def generic_table_definition():
        return {
            'TableName': str(uuid.uuid4()),
            'AttributeDefinitions': [{'AttributeName': 'id',
                                      'AttributeType': 'S'}],
            'KeySchema': [{'AttributeName': 'id', 'KeyType': 'HASH'}],
            'ProvisionedThroughput': {
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        }

    def get_client(self):
        return dynamodb.DynamoDB(endpoint=self.endpoint)


class AWSClientTests(AsyncTestCase):

    @testing.gen_test
    def test_raises_config_not_found_exception(self):
        with mock.patch('tornado_aws.client.AsyncAWSClient.fetch') as fetch:
            fetch.side_effect = aws_exceptions.ConfigNotFound(path='/test')
            with self.assertRaises(exceptions.ConfigNotFound):
                yield self.client.create_table(self.generic_table_definition())

    @testing.gen_test
    def test_raises_config_parser_error(self):
        with mock.patch('tornado_aws.client.AsyncAWSClient.fetch') as fetch:
            fetch.side_effect = aws_exceptions.ConfigParserError(path='/test')
            with self.assertRaises(exceptions.ConfigParserError):
                yield self.client.create_table(self.generic_table_definition())

    @testing.gen_test
    def test_raises_no_credentials_error(self):
        with mock.patch('tornado_aws.client.AsyncAWSClient.fetch') as fetch:
            fetch.side_effect = aws_exceptions.NoCredentialsError()
            with self.assertRaises(exceptions.NoCredentialsError):
                yield self.client.create_table(self.generic_table_definition())

    @testing.gen_test
    def test_raises_no_profile_error(self):
        with mock.patch('tornado_aws.client.AsyncAWSClient.fetch') as fetch:
            fetch.side_effect = aws_exceptions.NoProfileError(profile='test-1',
                                                              path='/test')
            with self.assertRaises(exceptions.NoProfileError):
                yield self.client.create_table(self.generic_table_definition())

    @testing.gen_test
    def test_raises_request_exception(self):
        with mock.patch('tornado_aws.client.AsyncAWSClient.fetch') as fetch:
            fetch.side_effect = httpclient.HTTPError(500, 'uh-oh')
            with self.assertRaises(exceptions.RequestException):
                yield self.client.create_table(self.generic_table_definition())

    @testing.gen_test
    def test_raises_timeout_exception(self):
        with mock.patch('tornado_aws.client.AsyncAWSClient.fetch') as fetch:
            fetch.side_effect = httpclient.HTTPError(599)
            with self.assertRaises(exceptions.TimeoutException):
                yield self.client.create_table(self.generic_table_definition())

    @testing.gen_test
    def test_fetch_future_exception(self):
        with mock.patch('tornado_aws.client.AsyncAWSClient.fetch') as fetch:
            future = concurrent.Future()
            fetch.return_value = future
            future.set_exception(exceptions.DynamoDBException())
            with self.assertRaises(exceptions.DynamoDBException):
               yield self.client.create_table(self.generic_table_definition())

    @testing.gen_test
    def test_empty_fetch_response_raises_dynamodb_exception(self):
        with mock.patch('tornado_aws.client.AsyncAWSClient.fetch') as fetch:
            future = concurrent.Future()
            fetch.return_value = future
            future.set_result(None)
            with self.assertRaises(exceptions.DynamoDBException):
                yield self.client.create_table(self.generic_table_definition())

    @testing.gen_test
    def test_gaierror_raises_request_exception(self):
        with mock.patch('tornado_aws.client.AsyncAWSClient.fetch') as fetch:
            fetch.side_effect = socket.gaierror
            with self.assertRaises(exceptions.RequestException):
                yield self.client.create_table(self.generic_table_definition())

    @unittest.skipIf(sys.version_info.major < 3,
                     'ConnectionError is Python3 only')
    @testing.gen_test
    def test_connection_error_request_exception(self):
        with mock.patch('tornado_aws.client.AsyncAWSClient.fetch') as fetch:
            fetch.side_effect = ConnectionError
            with self.assertRaises(exceptions.RequestException):
                yield self.client.create_table(self.generic_table_definition())


class CreateTableTests(AsyncTestCase):

    @testing.gen_test
    def test_simple_table(self):
        definition = self.generic_table_definition()
        response = yield self.client.create_table(definition)
        self.assertEqual(response['TableName'], definition['TableName'])
        self.assertIn(response['TableStatus'],
                      [dynamodb.TABLE_ACTIVE,
                       dynamodb.TABLE_CREATING])

    @testing.gen_test
    def test_invalid_request(self):
        definition = {
            'TableName': str(uuid.uuid4()),
            'AttributeDefinitions': [{'AttributeName': 'id'}],
            'KeySchema': [],
            'ProvisionedThroughput': {
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        }
        with self.assertRaises(exceptions.ValidationException):
            yield self.client.create_table(definition)

    @testing.gen_test
    def test_double_create(self):
        definition = self.generic_table_definition()
        response = yield self.client.create_table(definition)
        self.assertEqual(response['TableName'], definition['TableName'])
        self.assertIn(response['TableStatus'],
                      [dynamodb.TABLE_ACTIVE,
                       dynamodb.TABLE_CREATING])
        with self.assertRaises(exceptions.ResourceInUse):
            response = yield self.client.create_table(definition)


class DeleteTableTests(AsyncTestCase):

    @testing.gen_test
    def test_delete_table(self):
        definition = self.generic_table_definition()
        response = yield self.client.create_table(definition)
        self.assertEqual(response['TableName'], definition['TableName'])
        yield self.client.delete_table(definition['TableName'])
        with self.assertRaises(exceptions.ResourceNotFound):
            yield self.client.describe_table(definition['TableName'])

    @testing.gen_test
    def test_table_not_found(self):
        table = str(uuid.uuid4())
        with self.assertRaises(exceptions.ResourceNotFound):
            yield self.client.delete_table(table)


class DescribeTableTests(AsyncTestCase):

    @testing.gen_test
    def test_describe_table(self):
        # Create the table first
        definition = self.generic_table_definition()
        response = yield self.client.create_table(definition)
        self.assertEqual(response['TableName'], definition['TableName'])

        # Describe the table
        response = yield self.client.describe_table(definition['TableName'])
        self.assertEqual(response['TableName'], definition['TableName'])
        self.assertEqual(response['TableStatus'],
                         dynamodb.TABLE_ACTIVE)

    @testing.gen_test
    def test_table_not_found(self):
        table = str(uuid.uuid4())
        with self.assertRaises(exceptions.ResourceNotFound):
            yield self.client.describe_table(table)


class ListTableTests(AsyncTestCase):

    @testing.gen_test
    def test_list_tables(self):
        # Create the table first
        definition = self.generic_table_definition()
        response = yield self.client.create_table(definition)
        self.assertEqual(response['TableName'], definition['TableName'])

        # Describe the table
        response = yield self.client.list_tables(limit=100)
        self.assertIn(definition['TableName'], response['TableNames'])


class PutGetDeleteTests(AsyncTestCase):

    @testing.gen_test
    def test_put_item(self):
        # Create the table first
        definition = self.generic_table_definition()
        response = yield self.client.create_table(definition)
        self.assertEqual(response['TableName'], definition['TableName'])

        row_id = uuid.uuid4()

        # Describe the table
        yield self.client.put_item(
            definition['TableName'],
            {'id': row_id, 'created_at': datetime.datetime.utcnow()})

        response = yield self.client.get_item(definition['TableName'],
                                              {'id': row_id})
        self.assertEqual(response['id'], row_id)
