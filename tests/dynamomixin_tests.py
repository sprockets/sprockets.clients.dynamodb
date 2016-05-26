import json
import unittest
import uuid

try:
    from unittest import mock
except ImportError:
    import mock


from tornado import concurrent, gen, ioloop, testing, web

from sprockets.clients.dynamodb import connector, mixins
from sprockets.clients.dynamodb import exceptions as dynamo_exceptions


class CRUDHandler(mixins.DynamoMixin):

    @gen.coroutine
    def get(self, *args, **kwargs):
        item = yield self.get_item(self.path_args[0],
                                   {'id': self.path_args[1]})
        self.write(json.dumps(item).encode('utf-8'))
        self.set_status(200)
        self.finish()

    @gen.coroutine
    def put(self, *args, **kwargs):
        yield self.put_item(self.path_args[0],
                            json.loads(self.request.body.decode('utf-8')))
        self.set_status(200)
        self.finish()

    @gen.coroutine
    def post(self, *args, **kwargs):
        yield self.dynamodb_execute(
            self.path_args[0],
            json.loads(self.request.body.decode('utf-8')))


class _CommonOperationTests(testing.AsyncHTTPTestCase):
    num_retries = 2
    retry_after = 3

    def setUp(self):
        self.application = None
        super(_CommonOperationTests, self).setUp()

    def get_app(self):
        if self.application is None:
            self.application = web.Application(
                [web.url('/(.*)/(.*)', CRUDHandler),
                 web.url('/(.*)', CRUDHandler)],
                dynamodb={'max_retries': self.num_retries,
                          'retry_after': self.retry_after})
            setattr(self.application, 'dynamo', mock.Mock())
        return self.application

    @property
    def dynamo_mock(self):
        raise NotImplementedError

    def perform_action(self):
        raise NotImplementedError

    def test_that_operation_can_succeed(self):
        future = concurrent.Future()
        future.set_result({})
        self.dynamo_mock.return_value = future
        response = self.perform_action()
        self.assertEqual(response.code, 200)
        self.assertEqual(self.dynamo_mock.call_count, 1)

    def test_that_operation_returns_500_on_dynamodb_failure(self):
        self.dynamo_mock.side_effect = dynamo_exceptions.DynamoDBException
        response = self.perform_action()
        self.assertEqual(response.code, 500)
        self.assertEqual(self.dynamo_mock.call_count, 1)

    def test_that_operation_retries_request_exceptions(self):
        self.dynamo_mock.side_effect = dynamo_exceptions.RequestException
        response = self.perform_action()
        self.assertEqual(response.code, 500)
        self.assertEqual(self.dynamo_mock.call_count, self.num_retries + 1)

    def test_that_operation_retry_can_succeed(self):
        future = concurrent.Future()
        future.set_result({})
        self.dynamo_mock.side_effect = [dynamo_exceptions.RequestException,
                                        future]
        response = self.perform_action()
        self.assertEqual(response.code, 200)
        self.assertEqual(self.dynamo_mock.call_count, 2)

    def test_that_operation_credentials_error_returns_500(self):
        self.dynamo_mock.side_effect = dynamo_exceptions.NoCredentialsError
        response = self.perform_action()
        self.assertEqual(response.code, 500)
        self.assertEqual(self.dynamo_mock.call_count, 1)

    def test_that_operation_timeout_returns_500(self):
        self.dynamo_mock.side_effect = dynamo_exceptions.TimeoutException
        response = self.perform_action()
        self.assertEqual(response.code, 500)
        self.assertEqual(self.dynamo_mock.call_count, 1)

    def test_that_operation_rate_limits(self):
        self.dynamo_mock.side_effect = dynamo_exceptions.ThroughputExceeded
        response = self.perform_action()
        self.assertEqual(response.code, 429)
        self.assertEqual(self.dynamo_mock.call_count, 1)
        self.assertEqual(response.headers['Retry-After'],
                         str(self.retry_after))


class GetItemTests(_CommonOperationTests):

    @property
    def dynamo_mock(self):
        return self.application.dynamo.get_item

    def perform_action(self):
        return self.fetch('/table/key')


class PutItemTests(_CommonOperationTests):

    @property
    def dynamo_mock(self):
        return self.application.dynamo.put_item

    def perform_action(self):
        return self.fetch('/table', method='PUT', body='{"one":2}',
                          headers={'Content-Type': 'application/json'})


class DynamoDBExecuteTests(_CommonOperationTests):

    @property
    def dynamo_mock(self):
        return self.application.dynamo.execute

    def perform_action(self):
        return self.fetch('/operation', method='POST', body='"something"',
                          headers={'Content-Type': 'application/json'})


@gen.coroutine
def create_table(dynamodb, tabledef):
    max_attempts = 10
    first_time = True
    response = {}

    while not response.get('TableStatus') == 'Active':
        try:
            response = yield dynamodb.describe_table(tabledef['TableName'])
        except dynamo_exceptions.ResourceNotFound:
            if first_time:
                first_time = False
                response = yield dynamodb.create_table(tabledef)
        yield gen.sleep(0.25)

        max_attempts -= 1
        if max_attempts <= 0:
            raise RuntimeError('Failed to create table')


@unittest.skip('Currently broken in this project...')
class RoundTripTests(testing.AsyncHTTPTestCase):

    @classmethod
    def setUpClass(cls):
        super(RoundTripTests, cls).setUpClass()
        cls.table_name = uuid.uuid4().hex
        tabledef = {
            'TableName': cls.table_name,
            'AttributeDefinitions': [
                {'AttributeName': 'id', 'AttributeType': 'S'}],
            'KeySchema': [{'AttributeName': 'id', 'KeyType': 'HASH'}],
            'ProvisionedThroughput': {'ReadCapacityUnits': 1,
                                      'WriteCapacityUnits': 1},
        }
        iol = ioloop.IOLoop.instance()
        dynamo = connector.DynamoDB()
        iol.add_future(create_table(dynamo, tabledef),
                       lambda f: iol.stop())
        iol.start()

    @classmethod
    def tearDownClass(cls):
        super(RoundTripTests, cls).tearDownClass()
        iol = ioloop.IOLoop.instance()
        dynamo = connector.DynamoDB()
        iol.add_future(dynamo.delete_table(cls.table_name),
                       lambda f: iol.stop())
        iol.start()

    def setUp(self):
        self.application = None
        super(RoundTripTests, self).setUp()

    def get_app(self):
        if self.application is None:
            self.application = web.Application(
                [web.url('/(.*)/(.*)', CRUDHandler),
                 web.url('/(.*)', CRUDHandler)],
                dynamodb={})
            setattr(self.application, 'dynamo', connector.DynamoDB())
        return self.application

    def test_that_item_can_created_and_fetched(self):
        object_id = str(uuid.uuid4())
        response = self.fetch(
            '/{}'.format(self.table_name), method='PUT',
            body=json.dumps({'id': object_id}).encode('utf-8'),
            headers={'Content-Type': 'application/json'})
        self.assertEqual(response.code, 200)

        response = self.fetch(
            '/{}/{}'.format(self.table_name, object_id),
            headers={'Accept': 'application/json'})
        self.assertEqual(response.code, 200)
        self.assertEqual(json.loads(response.body.decode('utf-8')),
                         {'id': object_id})
