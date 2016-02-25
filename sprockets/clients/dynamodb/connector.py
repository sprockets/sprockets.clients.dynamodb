import json
import logging
import os

from tornado import concurrent, ioloop
from tornado_aws import client

from . import utils


LOGGER = logging.getLogger(__name__)


class DynamoDB(object):
    """
    Connects to a DynamoDB instance.

    :keyword str region: AWS region to send requests to
    :keyword str access_key: AWS access key.  If unspecified, this
        defaults to the :envvar:`AWS_ACCESS_KEY_ID` environment
        variable and will fall back to using the AWS CLI credentials
        file.  See :class:`tornado_aws.client.AsyncAWSClient` for
        more details.
    :keyword str secret_key: AWS secret used to secure API calls.
        If unspecified, this defaults to the :envvar:`AWS_SECRET_ACCESS_KEY`
        environment variable and will fall back to using the AWS CLI
        credentials as described in :class:`tornado_aws.client.AsyncAWSClient`.
    :keyword str profile: optional profile to use in AWS API calls.
        If unspecified, this defaults to the :envvar:`AWS_DEFAULT_PROFILE`
        environment variable or ``default`` if unset.
    :keyword str endpoint: DynamoDB endpoint to contact.  If unspecified,
        the default is determined by the region.
    :keyword int max_clients: optional maximum number of HTTP requests
        that may be performed in parallel.

    Create an instance of this class to interact with a DynamoDB
    server.  A :class:`tornado_aws.client.AsyncAWSClient` instance
    implements the AWS API wrapping and this class provides the
    DynamoDB specifics.

    """

    def __init__(self, **kwargs):
        self.logger = LOGGER.getChild(self.__class__.__name__)
        self._client = None
        self._args = kwargs.copy()
        if os.environ.get('DYNAMODB_ENDPOINT', None):
            self._args.setdefault('endpoint', os.environ['DYNAMODB_ENDPOINT'])

    @property
    def client(self):
        if self._client is None:
            self._client = client.AsyncAWSClient('dynamodb', **self._args)
        return self._client

    def execute(self, function, body):
        """
        Invoke a DynamoDB function.

        :param str function: DynamoDB function to invoke
        :param dict body: body to send with the function
        :rtype: tornado.concurrent.Future

        This method creates a future that will resolve to the result
        of calling the specified DynamoDB function.  It does it's best
        to unwrap the response from the function to make life a little
        easier for you.  It does this for the ``GetItem`` and ``Query``
        functions currrently.

        """
        encoded = json.dumps(body).encode('utf-8')
        headers = {
            'x-amz-target': 'DynamoDB_20120810.{}'.format(function),
            'Content-Type': 'application/x-amz-json-1.0',
        }
        future = concurrent.TracebackFuture()

        def handle_response(f):
            self.logger.debug('processing %s() = %r', function, f)
            try:
                response = f.result()
                result = json.loads(response.body.decode('utf-8'))
                future.set_result(_unwrap_result(function, result))
            except Exception as exception:
                future.set_exception(exception)

        self.logger.debug('calling %s', function)
        aws_response = self.client.fetch('POST', '/', body=encoded,
                                         headers=headers)
        ioloop.IOLoop.current().add_future(aws_response, handle_response)

        return future

    def create_table(self, table_definition):
        """
        Invoke the ``CreateTable`` function.

        :param dict table_definition: description of the table to
            create according to `CreateTable`_
        :rtype: tornado.concurrent.Future

        .. _CreateTable: http://docs.aws.amazon.com/amazondynamodb/
           latest/APIReference/API_CreateTable.html

        """
        return self.execute('CreateTable', table_definition)

    def describe_table(self, table_name):
        """
        Invoke the `DescribeTable`_ function.

        :param str table_name: name of the table to describe.
        :rtype: tornado.concurrent.Future

        .. _DescribeTable: http://docs.aws.amazon.com/amazondynamodb/
           latest/APIReference/API_DescribeTable.html

        """
        return self.execute('DescribeTable', {'TableName': table_name})

    def delete_table(self, table_name):
        """
        Invoke the `DeleteTable`_ function.

        :param str table_name: name of the table to describe.
        :rtype: tornado.concurrent.Future

        .. _DeleteTable: http://docs.aws.amazon.com/amazondynamodb/
           latest/APIReference/API_DeleteTable.html

        """
        return self.execute('DeleteTable', {'TableName': table_name})

    def put_item(self, table_name, item):
        """
        Invoke the `PutItem`_ function.

        :param str table_name: table to insert into
        :param dict item: item to insert.  This will be marshalled
            for you so a native :class:`dict` of native items works.
        :rtype: tornado.concurrent.Future

        .. _PutItem: http://docs.aws.amazon.com/amazondynamodb/
           latest/APIReference/API_PutItem.html

        """
        return self.execute('PutItem', {'TableName': table_name,
                                        'Item': utils.marshall(item)})

    def get_item(self, table_name, key_dict):
        """
        Invoke the `GetItem`_ function.

        :param str table_name: table to retrieve the item from
        :param dict key_dict: key to use for retrieval.  This will
            be marshalled for you so a native :class:`dict` works.
        :rtype: tornado.concurrent.Future

        .. _GetItem: http://docs.aws.amazon.com/amazondynamodb/
           latest/APIReference/API_GetItem.html

        """
        return self.execute('GetItem', {'TableName': table_name,
                                        'Key': utils.marshall(key_dict)})


def _unwrap_result(function, result):
    if result:
        if function == 'GetItem':
            return utils.unmarshall(result['Item'])
        if function == 'Query':
            return [utils.unmarshall(item) for item in result['Items']]
    return result
