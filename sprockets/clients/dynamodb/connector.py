import json
import logging
import os

from tornado import concurrent, httpclient, ioloop
import tornado_aws
from tornado_aws import exceptions as aws_exceptions

from . import utils
from . import exceptions


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
            self._client = tornado_aws.AsyncAWSClient('dynamodb', **self._args)
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

        :raises: :exc:`~sprockets.clients.dynamodb.exceptions.DynamoDBException`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ConfigNotFound`
                 :exc:`~sprockets.clients.dynamodb.exceptions.NoCredentialsError`
                 :exc:`~sprockets.clients.dynamodb.exceptions.NoProfileError`
                 :exc:`~sprockets.clients.dynamodb.exceptions.TimeoutException`
                 :exc:`~sprockets.clients.dynamodb.exceptions.RequestException`
                 :exc:`~sprockets.clients.dynamodb.exceptions.InternalFailure`
                 :exc:`~sprockets.clients.dynamodb.exceptions.LimitExceeded`
                 :exc:`~sprockets.clients.dynamodb.exceptions.MissingParameter`
                 :exc:`~sprockets.clients.dynamodb.exceptions.OptInRequired`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ResourceInUse`
                 :exc:`~sprockets.clients.dynamodb.exceptions.RequestExpired`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ResourceNotFound`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ServiceUnavailable`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ThroughputExceeded`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ValidationException`

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
                result = self._process_response(f)
            except aws_exceptions.AWSError as aws_error:
                future.set_exception(exceptions.DynamoDBException(aws_error))
            except httpclient.HTTPError as http_err:
                if http_err.code == 599:
                    future.set_exception(exceptions.TimeoutException())
                else:
                    future.set_exception(
                        exceptions.RequestException(http_err.message))
            except Exception as exception:
                future.set_exception(exception)
            else:
                future.set_result(_unwrap_result(function, result))

        try:
            aws_response = self.client.fetch('POST', '/', body=encoded,
                                             headers=headers)
        except aws_exceptions.ConfigNotFound as error:
            future.set_exception(exceptions.ConfigNotFound(str(error)))
        except aws_exceptions.ConfigParserError as error:
            future.set_exception(exceptions.ConfigParserError(str(error)))
        except aws_exceptions.NoCredentialsError as error:
            future.set_exception(exceptions.NoCredentialsError(str(error)))
        except aws_exceptions.NoProfileError as error:
            future.set_exception(exceptions.NoProfileError(str(error)))
        except httpclient.HTTPError as err:
            if err.code == 599:
                future.set_exception(exceptions.TimeoutException())
            else:
                future.set_exception(exceptions.RequestException(err.message))
        else:
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

        :raises: :exc:`~sprockets.clients.dynamodb.exceptions.DynamoDBException`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ConfigNotFound`
                 :exc:`~sprockets.clients.dynamodb.exceptions.NoCredentialsError`
                 :exc:`~sprockets.clients.dynamodb.exceptions.NoProfileError`
                 :exc:`~sprockets.clients.dynamodb.exceptions.TimeoutException`
                 :exc:`~sprockets.clients.dynamodb.exceptions.RequestException`
                 :exc:`~sprockets.clients.dynamodb.exceptions.InternalFailure`
                 :exc:`~sprockets.clients.dynamodb.exceptions.LimitExceeded`
                 :exc:`~sprockets.clients.dynamodb.exceptions.MissingParameter`
                 :exc:`~sprockets.clients.dynamodb.exceptions.OptInRequired`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ResourceInUse`
                 :exc:`~sprockets.clients.dynamodb.exceptions.RequestExpired`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ServiceUnavailable`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ValidationException`

        """
        future = concurrent.TracebackFuture()

        def handle_response(response):
            exception = response.exception()
            if exception:
                future.set_exception(exception)
            else:
                future.set_result(response.result()['TableDescription'])

        aws_response = self.execute('CreateTable', table_definition)
        ioloop.IOLoop.current().add_future(aws_response, handle_response)
        return future

    def update_table(self, table_definition):
        """
        Modifies the provisioned throughput settings, global secondary
        indexes, or DynamoDB Streams settings for a given table.

        You can only perform one of the following operations at once:

        - Modify the provisioned throughput settings of the table.
        - Enable or disable Streams on the table.
        - Remove a global secondary index from the table.
        - Create a new global secondary index on the table. Once the index
          begins backfilling, you can use *UpdateTable* to perform other
          operations.

        *UpdateTable* is an asynchronous operation; while it is executing, the
        table status changes from ``ACTIVE`` to ``UPDATING``. While it is
        ``UPDATING``, you cannot issue another *UpdateTable* request. When the
        table returns to the ``ACTIVE`` state, the *UpdateTable* operation is
        complete.

        :param dict table_definition: description of the table to
            update according to `UpdateTable`_
        :rtype: tornado.concurrent.Future

        :raises: :exc:`~sprockets.clients.dynamodb.exceptions.DynamoDBException`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ConfigNotFound`
                 :exc:`~sprockets.clients.dynamodb.exceptions.NoCredentialsError`
                 :exc:`~sprockets.clients.dynamodb.exceptions.NoProfileError`
                 :exc:`~sprockets.clients.dynamodb.exceptions.TimeoutException`
                 :exc:`~sprockets.clients.dynamodb.exceptions.RequestException`
                 :exc:`~sprockets.clients.dynamodb.exceptions.InternalFailure`
                 :exc:`~sprockets.clients.dynamodb.exceptions.LimitExceeded`
                 :exc:`~sprockets.clients.dynamodb.exceptions.MissingParameter`
                 :exc:`~sprockets.clients.dynamodb.exceptions.OptInRequired`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ResourceInUse`
                 :exc:`~sprockets.clients.dynamodb.exceptions.RequestExpired`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ServiceUnavailable`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ValidationException`

        .. _UpdateTable: http://docs.aws.amazon.com/amazondynamodb/
           latest/APIReference/API_UpdateTable.html

        """
        raise NotImplementedError

    def delete_table(self, table_name):
        """
        Invoke the `DeleteTable`_ function. The DeleteTable operation deletes a
        table and all of its items. After a DeleteTable request, the specified
        table is in the DELETING state until DynamoDB completes the deletion.
        If the table is in the ACTIVE state, you can delete it. If a table is
        in CREATING or UPDATING states, then a
        :py:exc:`~sprockets.clients.dynamodb.exceptions.ResourceInUse`
        exception is raised. If the  specified table does not exist, a
        :exc:`~sprockets.clients.dynamodb.exceptions.ResourceNotFound`
        exception is raised. If table is already in the DELETING state, no
        error is returned.

        :param str table_name: name of the table to describe.
        :rtype: tornado.concurrent.Future
        :returns: Response Format:

            .. code:: json

                {
                  "AttributeDefinitions": [{
                    "AttributeName": "string",
                    "AttributeType": "string"
                  }],
                  "CreationDateTime": number,
                  "GlobalSecondaryIndexes": [{
                    "Backfilling": boolean,
                    "IndexArn": "string",
                    "IndexName": "string",
                    "IndexSizeBytes": number,
                    "IndexStatus": "string",
                    "ItemCount": number,
                    "KeySchema": [{
                      "AttributeName": "string",
                      "KeyType": "string"
                    }],
                    "Projection": {
                      "NonKeyAttributes": [
                        "string"
                      ],
                      "ProjectionType": "string"
                    },
                    "ProvisionedThroughput": {
                      "LastDecreaseDateTime": number,
                      "LastIncreaseDateTime": number,
                      "NumberOfDecreasesToday": number,
                      "ReadCapacityUnits": number,
                      "WriteCapacityUnits": number
                    }
                  }],
                  "ItemCount": number,
                  "KeySchema": [{
                    "AttributeName": "string",
                    "KeyType": "string"
                  }],
                  "LatestStreamArn": "string",
                  "LatestStreamLabel": "string",
                  "LocalSecondaryIndexes": [{
                    "IndexArn": "string",
                    "IndexName": "string",
                    "IndexSizeBytes": number,
                    "ItemCount": number,
                    "KeySchema": [{
                      "AttributeName": "string",
                      "KeyType": "string"
                    }],
                    "Projection": {
                      "NonKeyAttributes": [
                        "string"
                      ],
                      "ProjectionType": "string"
                    }
                  }],
                  "ProvisionedThroughput": {
                    "LastDecreaseDateTime": number,
                    "LastIncreaseDateTime": number,
                    "NumberOfDecreasesToday": number,
                    "ReadCapacityUnits": number,
                    "WriteCapacityUnits": number
                  },
                  "StreamSpecification": {
                    "StreamEnabled": boolean,
                    "StreamViewType": "string"
                  },
                  "TableArn": "string",
                  "TableName": "string",
                  "TableSizeBytes": number,
                  "TableStatus": "string"
                }

        :raises: :exc:`~sprockets.clients.dynamodb.exceptions.DynamoDBException`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ConfigNotFound`
                 :exc:`~sprockets.clients.dynamodb.exceptions.NoCredentialsError`
                 :exc:`~sprockets.clients.dynamodb.exceptions.NoProfileError`
                 :exc:`~sprockets.clients.dynamodb.exceptions.TimeoutException`
                 :exc:`~sprockets.clients.dynamodb.exceptions.RequestException`
                 :exc:`~sprockets.clients.dynamodb.exceptions.InternalFailure`
                 :exc:`~sprockets.clients.dynamodb.exceptions.LimitExceeded`
                 :exc:`~sprockets.clients.dynamodb.exceptions.MissingParameter`
                 :exc:`~sprockets.clients.dynamodb.exceptions.OptInRequired`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ResourceInUse`
                 :exc:`~sprockets.clients.dynamodb.exceptions.RequestExpired`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ServiceUnavailable`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ValidationException`

        .. _DeleteTable: http://docs.aws.amazon.com/amazondynamodb/
           latest/APIReference/API_DeleteTable.html

        """
        return self.execute('DeleteTable', {'TableName': table_name})

    def describe_table(self, table_name):
        """
        Invoke the `DescribeTable`_ function.

        :param str table_name: name of the table to describe.
        :rtype: tornado.concurrent.Future
        :returns: Response Format:

            .. code:: json

                {
                  "AttributeDefinitions": [{
                    "AttributeName": "string",
                    "AttributeType": "string"
                  }],
                  "CreationDateTime": number,
                  "GlobalSecondaryIndexes": [{
                    "Backfilling": boolean,
                    "IndexArn": "string",
                    "IndexName": "string",
                    "IndexSizeBytes": number,
                    "IndexStatus": "string",
                    "ItemCount": number,
                    "KeySchema": [{
                      "AttributeName": "string",
                      "KeyType": "string"
                    }],
                    "Projection": {
                      "NonKeyAttributes": [
                        "string"
                      ],
                      "ProjectionType": "string"
                    },
                    "ProvisionedThroughput": {
                      "LastDecreaseDateTime": number,
                      "LastIncreaseDateTime": number,
                      "NumberOfDecreasesToday": number,
                      "ReadCapacityUnits": number,
                      "WriteCapacityUnits": number
                    }
                  }],
                  "ItemCount": number,
                  "KeySchema": [{
                    "AttributeName": "string",
                    "KeyType": "string"
                  }],
                  "LatestStreamArn": "string",
                  "LatestStreamLabel": "string",
                  "LocalSecondaryIndexes": [{
                    "IndexArn": "string",
                    "IndexName": "string",
                    "IndexSizeBytes": number,
                    "ItemCount": number,
                    "KeySchema": [{
                      "AttributeName": "string",
                      "KeyType": "string"
                    }],
                    "Projection": {
                      "NonKeyAttributes": [
                        "string"
                      ],
                      "ProjectionType": "string"
                    }
                  }],
                  "ProvisionedThroughput": {
                    "LastDecreaseDateTime": number,
                    "LastIncreaseDateTime": number,
                    "NumberOfDecreasesToday": number,
                    "ReadCapacityUnits": number,
                    "WriteCapacityUnits": number
                  },
                  "StreamSpecification": {
                    "StreamEnabled": boolean,
                    "StreamViewType": "string"
                  },
                  "TableArn": "string",
                  "TableName": "string",
                  "TableSizeBytes": number,
                  "TableStatus": "string"
                }

        :raises: :exc:`~sprockets.clients.dynamodb.exceptions.DynamoDBException`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ConfigNotFound`
                 :exc:`~sprockets.clients.dynamodb.exceptions.NoCredentialsError`
                 :exc:`~sprockets.clients.dynamodb.exceptions.NoProfileError`
                 :exc:`~sprockets.clients.dynamodb.exceptions.TimeoutException`
                 :exc:`~sprockets.clients.dynamodb.exceptions.RequestException`
                 :exc:`~sprockets.clients.dynamodb.exceptions.InternalFailure`
                 :exc:`~sprockets.clients.dynamodb.exceptions.LimitExceeded`
                 :exc:`~sprockets.clients.dynamodb.exceptions.MissingParameter`
                 :exc:`~sprockets.clients.dynamodb.exceptions.OptInRequired`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ResourceInUse`
                 :exc:`~sprockets.clients.dynamodb.exceptions.RequestExpired`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ServiceUnavailable`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ValidationException`

        .. _DescribeTable: http://docs.aws.amazon.com/amazondynamodb/
           latest/APIReference/API_DescribeTable.html

        """
        future = concurrent.TracebackFuture()

        def handle_response(response):
            exception = response.exception()
            if exception:
                future.set_exception(exception)
            else:
                future.set_result(response.result()['Table'])

        aws_response = self.execute('DescribeTable', {'TableName': table_name})
        ioloop.IOLoop.current().add_future(aws_response, handle_response)
        return future

    def list_tables(self, exclusive_start_table_name=None, limit=None):
        """
        Invoke the `ListTables`_ function.

        Returns an array of table names associated with the current account
        and endpoint. The output from *ListTables* is paginated, with each page
        returning a maximum of ``100`` table names.

        :param str exclusive_start_table_name: The first table name that this
            operation will evaluate. Use the value that was returned for
            ``LastEvaluatedTableName`` in a previous operation, so that you can
            obtain the next page of results.
        :param int limit: A maximum number of table names to return. If this
            parameter is not specified, the limit is ``100``.
        :returns: Response Format:

            .. code:: json

                {
                  "LastEvaluatedTableName": "string",
                  "TableNames": [
                    "string"
                  ]
                }

        :raises: :exc:`~sprockets.clients.dynamodb.exceptions.DynamoDBException`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ConfigNotFound`
                 :exc:`~sprockets.clients.dynamodb.exceptions.NoCredentialsError`
                 :exc:`~sprockets.clients.dynamodb.exceptions.NoProfileError`
                 :exc:`~sprockets.clients.dynamodb.exceptions.TimeoutException`
                 :exc:`~sprockets.clients.dynamodb.exceptions.RequestException`
                 :exc:`~sprockets.clients.dynamodb.exceptions.InternalFailure`
                 :exc:`~sprockets.clients.dynamodb.exceptions.LimitExceeded`
                 :exc:`~sprockets.clients.dynamodb.exceptions.MissingParameter`
                 :exc:`~sprockets.clients.dynamodb.exceptions.OptInRequired`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ResourceInUse`
                 :exc:`~sprockets.clients.dynamodb.exceptions.RequestExpired`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ServiceUnavailable`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ValidationException`

        .. _ListTables: http://docs.aws.amazon.com/amazondynamodb/
           latest/APIReference/API_ListTables.html

        """
        payload = {}
        if exclusive_start_table_name:
            payload['ExclusiveStartTableName'] = exclusive_start_table_name
        if limit:
            payload['Limit'] = limit
        return self.execute('ListTables', payload)

    def put_item(self, table_name, item, return_values=False,
                 condition_expression=None,
                 expression_attribute_names=None,
                 expression_attribute_values=None,
                 return_consumed_capacity=None,
                 return_item_collection_metrics=False):
        """Invoke the `PutItem`_ function, creating a new item, or replaces an
        old item with a new item. If an item that has the same primary key as
        the new item already exists in the specified table, the new item
        completely replaces the existing item. You can perform a conditional
        put operation (add a new item if one with the specified primary key
        doesn't exist), or replace an existing item if it has certain attribute
        values.

        In addition to putting an item, you can also return the item's
        attribute values in the same operation, using the ``return_values``
        parameter.

        When you add an item, the primary key attribute(s) are the only
        required attributes. Attribute values cannot be null. String and Binary
        type attributes must have lengths greater than zero. Set type
        attributes cannot be empty. Requests with empty values will be rejected
        with a
        :exc:`~sprockets.clients.dynamodb.exceptions.ValidationException`.

        You can request that PutItem return either a copy of the original item
        (before the update) or a copy of the updated item (after the update).
        For more information, see the ReturnValues description below.

        .. note:: To prevent a new item from replacing an existing item, use a
            conditional expression that contains the attribute_not_exists
            function with the name of the attribute being used as the partition
            key for the table. Since every record must contain that attribute,
            the attribute_not_exists function will only succeed if no matching
            item exists.

        For more information about using this API, see Working with Items in
        the Amazon DynamoDB Developer Guide.

        :param str table_name: The table to put the item to
        :param dict item: A map of attribute name/value pairs, one for each
            attribute. Only the primary key attributes are required; you can
            optionally provide other attribute name-value pairs for the item.

            You must provide all of the attributes for the primary key. For
            example, with a simple primary key, you only need to provide a
            value for the partition key. For a composite primary key, you must
            provide both values for both the partition key and the sort key.

            If you specify any attributes that are part of an index key, then
            the data types for those attributes must match those of the schema
            in the table's attribute definition.
        :param bool return_values: Set to ``True`` if you want to get the item
            attributes as they appeared before they were updated with the
            *PutItem* request.
        :param str condition_expression: A condition that must be satisfied in
            order for a conditional *PutItem* operation to succeed. See the
            `AWS documentation for ConditionExpression <http://docs.aws.amazon.
            com/amazondynamodb/latest/APIReference/API_PutItem.html#DDB-Put
            Item-request-ConditionExpression>`_ for more information.
        :param dict expression_attribute_names: One or more substitution tokens
            for attribute names in an expression. See the `AWS documentation
            for ExpressionAttributeNames <http://docs.aws.amazon.com/amazon
            dynamodb/latest/APIReference/API_PutItem.html#DDB-PutItem-request-
            ExpressionAttributeNames>`_ for more information.
        :param dict expression_attribute_values: One or more values that can be
            substituted in an expression. See the `AWS documentation
            for ExpressionAttributeValues <http://docs.aws.amazon.com/amazon
            dynamodb/latest/APIReference/API_PutItem.html#DDB-PutItem-request-
            ExpressionAttributeValues>`_ for more information.
        :param str return_consumed_capacity: Determines the level of detail
            about provisioned throughput consumption that is returned in the
            response. Should be ``None`` or one of ``INDEXES`` or ``TOTAL``
        :param bool return_item_collection_metrics: Determines whether item
            collection metrics are returned.
        :rtype: tornado.concurrent.Future

        :raises: :exc:`~sprockets.clients.dynamodb.exceptions.DynamoDBException`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ConfigNotFound`
                 :exc:`~sprockets.clients.dynamodb.exceptions.NoCredentialsError`
                 :exc:`~sprockets.clients.dynamodb.exceptions.NoProfileError`
                 :exc:`~sprockets.clients.dynamodb.exceptions.TimeoutException`
                 :exc:`~sprockets.clients.dynamodb.exceptions.RequestException`
                 :exc:`~sprockets.clients.dynamodb.exceptions.InternalFailure`
                 :exc:`~sprockets.clients.dynamodb.exceptions.LimitExceeded`
                 :exc:`~sprockets.clients.dynamodb.exceptions.MissingParameter`
                 :exc:`~sprockets.clients.dynamodb.exceptions.OptInRequired`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ResourceInUse`
                 :exc:`~sprockets.clients.dynamodb.exceptions.RequestExpired`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ServiceUnavailable`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ValidationException`

        .. _PutItem: http://docs.aws.amazon.com/amazondynamodb/
           latest/APIReference/API_PutItem.html

        """
        payload = {'TableName': table_name, 'Item': utils.marshall(item)}
        if condition_expression:
            payload['ConditionExpression'] = condition_expression
        if expression_attribute_names:
            payload['ExpressionAttributeNames'] = expression_attribute_names
        if expression_attribute_values:
            payload['ExpressionAttributeValues'] = expression_attribute_values
        if return_consumed_capacity:
            payload['ReturnConsumedCapacity'] = return_consumed_capacity
        if return_item_collection_metrics:
            payload['ReturnItemCollectionMetrics'] = 'SIZE'
        if return_values:
            payload['ReturnValues'] = 'ALL_OLD'
        return self.execute('PutItem', payload)

    def get_item(self, table_name, key_dict, consistent_read=False,
                 expression_attribute_names=None,
                 projection_expression=None, return_consumed_capacity=None):
        """
        Invoke the `GetItem`_ function.

        :param str table_name: table to retrieve the item from
        :param dict key_dict: key to use for retrieval.  This will
            be marshalled for you so a native :class:`dict` works.
        :param bool consistent_read: Determines the read consistency model: If
            set to :py:data`True`, then the operation uses strongly consistent
            reads; otherwise, the operation uses eventually consistent reads.
        :param dict expression_attribute_names: One or more substitution tokens
            for attribute names in an expression.
        :param str projection_expression: A string that identifies one or more
            attributes to retrieve from the table. These attributes can include
            scalars, sets, or elements of a JSON document. The attributes in
            the expression must be separated by commas. If no attribute names
            are specified, then all attributes will be returned. If any of the
            requested attributes are not found, they will not appear in the
            result.
        :param str return_consumed_capacity: Determines the level of detail
            about provisioned throughput consumption that is returned in the
            response:

              - INDEXES: The response includes the aggregate consumed
                capacity for the operation, together with consumed capacity for
                each table and secondary index that was accessed. Note that
                some operations, such as *GetItem* and *BatchGetItem*, do not
                access any indexes at all. In these cases, specifying INDEXES
                will only return consumed capacity information for table(s).
              - TOTAL: The response includes only the aggregate consumed
                capacity for the operation.
              - NONE: No consumed capacity details are included in the
                response.
        :rtype: tornado.concurrent.Future

        :raises: :exc:`~sprockets.clients.dynamodb.exceptions.DynamoDBException`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ConfigNotFound`
                 :exc:`~sprockets.clients.dynamodb.exceptions.NoCredentialsError`
                 :exc:`~sprockets.clients.dynamodb.exceptions.NoProfileError`
                 :exc:`~sprockets.clients.dynamodb.exceptions.TimeoutException`
                 :exc:`~sprockets.clients.dynamodb.exceptions.RequestException`
                 :exc:`~sprockets.clients.dynamodb.exceptions.InternalFailure`
                 :exc:`~sprockets.clients.dynamodb.exceptions.LimitExceeded`
                 :exc:`~sprockets.clients.dynamodb.exceptions.MissingParameter`
                 :exc:`~sprockets.clients.dynamodb.exceptions.OptInRequired`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ResourceInUse`
                 :exc:`~sprockets.clients.dynamodb.exceptions.RequestExpired`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ResourceNotFound`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ServiceUnavailable`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ThroughputExceeded`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ValidationException`

        .. _GetItem: http://docs.aws.amazon.com/amazondynamodb/
           latest/APIReference/API_GetItem.html

        """
        payload = {'TableName': table_name,
                   'Key': utils.marshall(key_dict),
                   'ConsistentRead': consistent_read}
        if expression_attribute_names:
            payload['ExpressionAttributeNames'] = expression_attribute_names
        if projection_expression:
            payload['ProjectionExpression'] = projection_expression
        if return_consumed_capacity:
            payload['ReturnConsumedCapacity'] = return_consumed_capacity
        return self.execute('GetItem', payload)

    def update_item(self, table_name, key, return_values=False,
                    condition_expression=None, update_expression=None,
                    expression_attribute_names=None,
                    expression_attribute_values=None,
                    return_consumed_capacity=None,
                    return_item_collection_metrics=False):
        """Invoke the `UpdateItem`_ function.

        Edits an existing item's attributes, or adds a new item to the table
        if it does not already exist. You can put, delete, or add attribute
        values. You can also perform a conditional update on an existing item
        (insert a new attribute name-value pair if it doesn't exist, or replace
        an existing name-value pair if it has certain expected attribute
        values).

        :param str table_name: The name of the table that contains the item to
            update
        :param dict key: A dictionary of key/value pairs that are used to
            define the primary key values for the item. For the primary key,
            you must provide all of the attributes. For example, with a simple
            primary key, you only need to provide a value for the partition
            key. For a composite primary key, you must provide values for both
            the partition key and the sort key.
        :param bool return_values: Set to ``True`` if you want to get the item
            attributes as they appeared before they were updated with the
            *UpdateItem* request.
        :param str condition_expression: A condition that must be satisfied in
            order for a conditional *UpdateItem* operation to succeed. One of:
            ``attribute_exists``, ``attribute_not_exists``, ``attribute_type``,
            ``contains``, ``begins_with``, ``size``, ``=``, ``<>``, ``<``,
            ``>``, ``<=``, ``>=``, ``BETWEEN``, ``IN``, ``AND``, ``OR``, or
            ``NOT``.
        :param str update_expression: An expression that defines one or more
            attributes to be updated, the action to be performed on them, and
            new value(s) for them.
        :param dict expression_attribute_names: One or more substitution tokens
            for attribute names in an expression.
        :param dict expression_attribute_values: One or more values that can be
            substituted in an expression.
        :param str return_consumed_capacity: Determines the level of detail
            about provisioned throughput consumption that is returned in the
            response. Should be ``None`` or one of ``INDEXES`` or ``TOTAL``
        :param bool return_item_collection_metrics: Determines whether item
            collection metrics are returned.
        :rtype: dict

        :raises: :exc:`~sprockets.clients.dynamodb.exceptions.DynamoDBException`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ConfigNotFound`
                 :exc:`~sprockets.clients.dynamodb.exceptions.NoCredentialsError`
                 :exc:`~sprockets.clients.dynamodb.exceptions.NoProfileError`
                 :exc:`~sprockets.clients.dynamodb.exceptions.TimeoutException`
                 :exc:`~sprockets.clients.dynamodb.exceptions.RequestException`
                 :exc:`~sprockets.clients.dynamodb.exceptions.InternalFailure`
                 :exc:`~sprockets.clients.dynamodb.exceptions.LimitExceeded`
                 :exc:`~sprockets.clients.dynamodb.exceptions.MissingParameter`
                 :exc:`~sprockets.clients.dynamodb.exceptions.OptInRequired`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ResourceInUse`
                 :exc:`~sprockets.clients.dynamodb.exceptions.RequestExpired`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ResourceNotFound`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ServiceUnavailable`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ThroughputExceeded`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ValidationException`

        .. _GetItem: http://docs.aws.amazon.com/amazondynamodb/
           latest/APIReference/API_UpdateItem.html

        """
        raise NotImplementedError

    def delete_item(self, table_name, key, condition_expression=None,
                    expression_attribute_names=None,
                    expression_attribute_values=None,
                    return_consumed_capacity=None,
                    return_item_collection_metrics=False,
                    return_values=False):
        """Invoke the `DeleteItem`_ function that deletes a single item in a
        table by primary key. You can perform a conditional delete operation
        that deletes the item if it exists, or if it has an expected attribute
        value.

        In addition to deleting an item, you can also return the item's
        attribute values in the same operation, using the ``return_values``
        parameter.

        Unless you specify conditions, the *DeleteItem* is an idempotent
        operation; running it multiple times on the same item or attribute does
        not result in an error response.

        Conditional deletes are useful for deleting items only if specific
        conditions are met. If those conditions are met, DynamoDB performs the
        delete. Otherwise, the item is not deleted.

        :param str table_name: The name of the table from which to delete the
            item.
        :param dict key: A map of attribute names to ``AttributeValue``
            objects, representing the primary key of the item to delete. For
            the primary key, you must provide all of the attributes. For
            example, with a simple primary key, you only need to provide a
            value for the partition key. For a composite primary key, you must
            provide values for both the partition key and the sort key.
        :param str condition_expression: A condition that must be satisfied in
            order for a conditional *DeleteItem* to succeed. See the `AWS
            documentation for ConditionExpression <http://docs.aws.amazon.com/
            amazondynamodb/latest/APIReference/API_DeleteItem.html#DDB-Delete
            Item-request-ConditionExpression>`_ for more information.
        :param dict expression_attribute_names: One or more substitution tokens
            for attribute names in an expression. See the `AWS documentation
            for ExpressionAttributeNames <http://docs.aws.amazon.com/
            amazondynamodb/latest/APIReference/API_DeleteItem.html#DDB-Delete
            Item-request-ExpressionAttributeNames>`_ for more information.
        :param dict expression_attribute_values: One or more values that can be
            substituted in an expression. See the `AWS documentation
            for ExpressionAttributeValues <http://docs.aws.amazon.com/
            amazondynamodb/latest/APIReference/API_DeleteItem.html#DDB-Delete
            Item-request-ExpressionAttributeValues>`_ for more information.
        :param str return_consumed_capacity: Determines the level of detail
            about provisioned throughput consumption that is returned in the
            response. See the `AWS documentation
            for ReturnConsumedCapacity <http://docs.aws.amazon.com/
            amazondynamodb/latest/APIReference/API_DeleteItem.html#DDB-Delete
            Item-request-ReturnConsumedCapacity>`_ for more information.
        :param bool return_item_collection_metrics: Determines whether item
            collection metrics are returned.
        :param bool return_values: Return the item attributes as they appeared
            before they were deleted.
        :returns: Response format:

            .. code:: json

                {
                  "Attributes": {
                    "string": {
                      "B": blob,
                      "BOOL": boolean,
                      "BS": [
                        blob
                      ],
                      "L": [
                        AttributeValue
                      ],
                      "M": {
                        "string": AttributeValue
                      },
                      "N": "string",
                      "NS": [
                        "string"
                      ],
                      "NULL": boolean,
                      "S": "string",
                      "SS": [
                        "string"
                      ]
                    }
                  },
                  "ConsumedCapacity": {
                    "CapacityUnits": number,
                    "GlobalSecondaryIndexes": {
                      "string": {
                        "CapacityUnits": number
                      }
                    },
                    "LocalSecondaryIndexes": {
                      "string": {
                        "CapacityUnits": number
                      }
                    },
                    "Table": {
                      "CapacityUnits": number
                    },
                    "TableName": "string"
                  },
                  "ItemCollectionMetrics": {
                    "ItemCollectionKey": {
                      "string": {
                        "B": blob,
                        "BOOL": boolean,
                        "BS": [
                          blob
                        ],
                        "L": [
                          AttributeValue
                        ],
                        "M": {
                          "string": AttributeValue
                        },
                        "N": "string",
                        "NS": [
                          "string"
                        ],
                        "NULL": boolean,
                        "S": "string",
                        "SS": [
                          "string"
                        ]
                      }
                    },
                    "SizeEstimateRangeGB": [
                      number
                    ]
                  }
                }

        :raises: :exc:`~sprockets.clients.dynamodb.exceptions.DynamoDBException`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ConfigNotFound`
                 :exc:`~sprockets.clients.dynamodb.exceptions.NoCredentialsError`
                 :exc:`~sprockets.clients.dynamodb.exceptions.NoProfileError`
                 :exc:`~sprockets.clients.dynamodb.exceptions.TimeoutException`
                 :exc:`~sprockets.clients.dynamodb.exceptions.RequestException`
                 :exc:`~sprockets.clients.dynamodb.exceptions.InternalFailure`
                 :exc:`~sprockets.clients.dynamodb.exceptions.LimitExceeded`
                 :exc:`~sprockets.clients.dynamodb.exceptions.MissingParameter`
                 :exc:`~sprockets.clients.dynamodb.exceptions.OptInRequired`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ResourceInUse`
                 :exc:`~sprockets.clients.dynamodb.exceptions.RequestExpired`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ResourceNotFound`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ServiceUnavailable`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ThroughputExceeded`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ValidationException`

        .. _GetItem: http://docs.aws.amazon.com/amazondynamodb/
           latest/APIReference/API_DeleteItem.html

        """
        raise NotImplementedError

    def batch_get_item(self):
        """Invoke the `BatchGetItem`_ function.

        :raises: :exc:`~sprockets.clients.dynamodb.exceptions.DynamoDBException`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ConfigNotFound`
                 :exc:`~sprockets.clients.dynamodb.exceptions.NoCredentialsError`
                 :exc:`~sprockets.clients.dynamodb.exceptions.NoProfileError`
                 :exc:`~sprockets.clients.dynamodb.exceptions.TimeoutException`
                 :exc:`~sprockets.clients.dynamodb.exceptions.RequestException`
                 :exc:`~sprockets.clients.dynamodb.exceptions.InternalFailure`
                 :exc:`~sprockets.clients.dynamodb.exceptions.LimitExceeded`
                 :exc:`~sprockets.clients.dynamodb.exceptions.MissingParameter`
                 :exc:`~sprockets.clients.dynamodb.exceptions.OptInRequired`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ResourceInUse`
                 :exc:`~sprockets.clients.dynamodb.exceptions.RequestExpired`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ResourceNotFound`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ServiceUnavailable`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ThroughputExceeded`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ValidationException`

        .. _GetItem: http://docs.aws.amazon.com/amazondynamodb/
           latest/APIReference/API_BatchGetItem.html

        """
        raise NotImplementedError

    def batch_write_item(self):
        """Invoke the `BatchWriteItem`_ function.

        :raises: :exc:`~sprockets.clients.dynamodb.exceptions.DynamoDBException`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ConfigNotFound`
                 :exc:`~sprockets.clients.dynamodb.exceptions.NoCredentialsError`
                 :exc:`~sprockets.clients.dynamodb.exceptions.NoProfileError`
                 :exc:`~sprockets.clients.dynamodb.exceptions.TimeoutException`
                 :exc:`~sprockets.clients.dynamodb.exceptions.RequestException`
                 :exc:`~sprockets.clients.dynamodb.exceptions.InternalFailure`
                 :exc:`~sprockets.clients.dynamodb.exceptions.LimitExceeded`
                 :exc:`~sprockets.clients.dynamodb.exceptions.MissingParameter`
                 :exc:`~sprockets.clients.dynamodb.exceptions.OptInRequired`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ResourceInUse`
                 :exc:`~sprockets.clients.dynamodb.exceptions.RequestExpired`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ResourceNotFound`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ServiceUnavailable`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ThroughputExceeded`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ValidationException`

        .. _GetItem: http://docs.aws.amazon.com/amazondynamodb/
           latest/APIReference/API_BatchWriteItem.html

        """
        raise NotImplementedError

    def query(self, table_name, consistent_read=False,
              exclusive_start_key=None, expression_attribute_names=None,
              expression_attribute_values=None, filter_expression=None,
              projection_expression=None, index_name=None,limit=None,
              return_consumed_capacity=None, scan_index_forward=True,
              select=None):
        """A `Query`_ operation uses the primary key of a table or a secondary
        index to directly access items from that table or index.

        You can use the ``scan_index_forward`` parameter to get results in
        forward or reverse order, by sort key.

        Queries that do not return results consume the minimum number of read
        capacity units for that type of read operation.

        If the total number of items meeting the query criteria exceeds the
        result set size limit of 1 MB, the query stops and results are returned
        to the user with the ``LastEvaluatedKey`` element to continue the query
        in a subsequent operation. Unlike a *Scan* operation, a Query operation
        never returns both an empty result set and a ``LastEvaluatedKey``
        value. ``LastEvaluatedKey`` is only provided if the results exceed
        1 MB, or if you have used the ``limit`` parameter.

        You can query a table, a local secondary index, or a global secondary
        index. For a query on a table or on a local secondary index, you can
        set the ``consistent_read`` parameter to true and obtain a strongly
        consistent result. Global secondary indexes support eventually
        consistent reads only, so do not specify ``consistent_read`` when
        querying a global secondary index.

        :param str table_name: The name of the table containing the requested
            items.
        :param bool consistent_read: Determines the read consistency model: If
            set to ``True``, then the operation uses strongly consistent reads;
            otherwise, the operation uses eventually consistent reads. Strongly
            consistent reads are not supported on global secondary indexes. If
            you query a global secondary index with ``consistent_read`` set to
            ``True``, you will receive a
            :exc:`~tornado_dynamodb.exceptions.ValidationException`.
        :param str|bytes|int exclusive_start_key: The primary key of the first
            item that this operation will evaluate. Use the value that was
            returned for ``LastEvaluatedKey`` in the previous operation. In a
            parallel scan, a *Scan* request that includes
            ``exclusive_start_key`` must specify the same segment whose
            previous *Scan* returned the corresponding value of
            ``LastEvaluatedKey``.
        :param dict expression_attribute_names: One or more substitution tokens
            for attribute names in an expression.
        :param dict expression_attribute_values: One or more values that can be
            substituted in an expression.
        :param str filter_expression: A string that contains conditions that
            DynamoDB applies after the *Query* operation, but before the data
            is returned to you. Items that do not satisfy the criteria are not
            returned. Note that a filter expression is applied after the items
            have already been read; the process of filtering does not consume
            any additional read capacity units. For more information, see
            `Filter Expressions <http://docs.aws.amazon.com/amazondynamodb/
            latest/developerguide/QueryAndScan.html#FilteringResults>`_ in the
            Amazon DynamoDB Developer Guide.
        :param str projection_expression:
        :param str index_name: The name of a secondary index to query. This
            index can be any local secondary index or global secondary index.
            Note that if you use this parameter, you must also provide
            ``table_name``.
        :param int limit: The maximum number of items to evaluate (not
            necessarily the number of matching items). If DynamoDB processes
            the number of items up to the limit while processing the results,
            it stops the operation and returns the matching values up to that
            point, and a key in ``LastEvaluatedKey`` to apply in a subsequent
            operation, so that you can pick up where you left off. Also, if the
            processed data set size exceeds 1 MB before DynamoDB reaches this
            limit, it stops the operation and returns the matching values up to
            the limit, and a key in ``LastEvaluatedKey`` to apply in a
            subsequent operation to continue the operation. For more
            information, see `Query and Scan <http://docs.aws.amazon.com/amazo
            ndynamodb/latest/developerguide/QueryAndScan.html>`_ in the Amazon
            DynamoDB Developer Guide.
        :param str return_consumed_capacity: Determines the level of detail
            about provisioned throughput consumption that is returned in the
            response:

              - ``INDEXES``: The response includes the aggregate consumed
                capacity for the operation, together with consumed capacity for
                each table and secondary index that was accessed. Note that
                some operations, such as *GetItem* and *BatchGetItem*, do not
                access any indexes at all. In these cases, specifying
                ``INDEXES`` will only return consumed capacity information for
                table(s).
              - ``TOTAL``: The response includes only the aggregate consumed
                capacity for the operation.
              - ``NONE``: No consumed capacity details are included in the
                response.
        :param bool scan_index_forward: Specifies the order for index
            traversal: If ``True`` (default), the traversal is performed in
            ascending order; if ``False``, the traversal is performed in
            descending order. Items with the same partition key value are
            stored in sorted order by sort key. If the sort key data type is
            *Number*, the results are stored in numeric order. For type
            *String*, the results are stored in order of ASCII character code
            values. For type *Binary*, DynamoDB treats each byte of the binary
            data as unsigned. If set to ``True``, DynamoDB returns the results
            in the order in which they are stored (by sort key value). This is
            the default behavior. If set to ``False``, DynamoDB reads the
            results in reverse order by sort key value, and then returns the
            results to the client.
        :param str select: The attributes to be returned in the result. You can
            retrieve all item attributes, specific item attributes, the count
            of matching items, or in the case of an index, some or all of the
            attributes projected into the index. Possible values are:

              - ``ALL_ATTRIBUTES``: Returns all of the item attributes from the
                specified table or index. If you query a local secondary index,
                then for each matching item in the index DynamoDB will fetch
                the entire item from the parent table. If the index is
                configured to project all item attributes, then all of the data
                can be obtained from the local secondary index, and no fetching
                is required.
              - ``ALL_PROJECTED_ATTRIBUTES``: Allowed only when querying an
                index. Retrieves all attributes that have been projected into
                the index. If the index is configured to project all
                attributes, this return value is equivalent to specifying
                ``ALL_ATTRIBUTES``.
              - ``COUNT``: Returns the number of matching items, rather than
                the matching items themselves.
        :rtype: dict

        :raises: :exc:`~sprockets.clients.dynamodb.exceptions.DynamoDBException`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ConfigNotFound`
                 :exc:`~sprockets.clients.dynamodb.exceptions.NoCredentialsError`
                 :exc:`~sprockets.clients.dynamodb.exceptions.NoProfileError`
                 :exc:`~sprockets.clients.dynamodb.exceptions.TimeoutException`
                 :exc:`~sprockets.clients.dynamodb.exceptions.RequestException`
                 :exc:`~sprockets.clients.dynamodb.exceptions.InternalFailure`
                 :exc:`~sprockets.clients.dynamodb.exceptions.LimitExceeded`
                 :exc:`~sprockets.clients.dynamodb.exceptions.MissingParameter`
                 :exc:`~sprockets.clients.dynamodb.exceptions.OptInRequired`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ResourceInUse`
                 :exc:`~sprockets.clients.dynamodb.exceptions.RequestExpired`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ResourceNotFound`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ServiceUnavailable`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ThroughputExceeded`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ValidationException`

        .. _Query: http://docs.aws.amazon.com/amazondynamodb/
           latest/APIReference/API_Query.html

        """
        raise NotImplementedError

    def scan(self, table_name, consistent_read=False, exclusive_start_key=None,
             expression_attribute_names=None, expression_attribute_values=None,
             filter_expression=None, projection_expression=None,
             index_name=None, limit=None, return_consumed_capacity=None,
             segment=None, total_segments=None):
        """The `Scan`_ operation returns one or more items and item attributes
        by accessing every item in a table or a secondary index.

        If the total number of scanned items exceeds the maximum data set size
        limit of 1 MB, the scan stops and results are returned to the user as a
        ``LastEvaluatedKey`` value to continue the scan in a subsequent
        operation. The results also include the number of items exceeding the
        limit. A scan can result in no table data meeting the filter criteria.

        By default, Scan operations proceed sequentially; however, for faster
        performance on a large table or secondary index, applications can
        request a parallel *Scan* operation by providing the ``segment`` and
        ``total_segments`` parameters. For more information, see
        `Parallel Scan <http://docs.aws.amazon.com/amazondynamodb/latest/
        developerguide/QueryAndScan.html#QueryAndScanParallelScan>`_ in the
        Amazon DynamoDB Developer Guide.

        By default, *Scan* uses eventually consistent reads when accessing the
        data in a table; therefore, the result set might not include the
        changes to data in the table immediately before the operation began. If
        you need a consistent copy of the data, as of the time that the *Scan*
        begins, you can set the ``consistent_read`` parameter to ``True``.

        :param str table_name: The name of the table containing the requested
            items; or, if you provide IndexName, the name of the table to which
            that index belongs.
        :param bool consistent_read: A Boolean value that determines the read
            consistency model during the scan:

            - If set to ``False``, then the data returned from *Scan* might not
              contain the results from other recently completed write
              operations (*PutItem*, *UpdateItem*, or *DeleteItem*).
            - If set to ``True``, then all of the write operations that
              completed before the Scan began are guaranteed to be contained in
              the *Scan* response.

            The default setting is ``False``.

            This parameter is not supported on global secondary indexes. If you
            scan a global secondary index and set ``consistent_read`` to
            ``true``, you will receive a
            :exc:`~tornado_dynamodb.exceptions.ValidationException`.
        :param str|bytes|int exclusive_start_key: The primary key of the first
            item that this operation will evaluate. Use the value that was
            returned for ``LastEvaluatedKey`` in the previous operation.

            In a parallel scan, a *Scan* request that includes
            ``exclusive_start_key`` must specify the same segment whose
            previous *Scan* returned the corresponding value of
            ``LastEvaluatedKey``.
        :param dict expression_attribute_names: One or more substitution tokens
            for attribute names in an expression.
        :param dict expression_attribute_values: One or more values that can be
            substituted in an expression.
        :param str filter_expression: A string that contains conditions that
            DynamoDB applies after the Scan operation, but before the data is
            returned to you. Items that do not satisfy the expression criteria
            are not returned.

            .. note:: A filter expression is applied after the items have
                already been read; the process of filtering does not consume
                any additional read capacity units.

            For more information, see `Filter Expressions <http://docs.aws.
            amazon.com/amazondynamodb/latest/developerguide/QueryAndScan.html
            #FilteringResults>`_ in the Amazon DynamoDB Developer Guide.
        :param str projection_expression: A string that identifies one or more
            attributes to retrieve from the specified table or index. These
            attributes can include scalars, sets, or elements of a JSON
            document. The attributes in the expression must be separated by
            commas.

            If no attribute names are specified, then all attributes will be
            returned. If any of the requested attributes are not found, they
            will not appear in the result.

            For more information, see `Accessing Item Attributes <http://docs.
            aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.
            AccessingItemAttributes.html>`_ in the Amazon DynamoDB Developer
            Guide.
        :param str index_name: The name of a secondary index to scan. This
            index can be any local secondary index or global secondary index.
            Note that if you use this parameter, you must also provide
            ``table_name``.
        :param int limit: The maximum number of items to evaluate (not
            necessarily the number of matching items). If DynamoDB processes
            the number of items up to the limit while processing the results,
            it stops the operation and returns the matching values up to that
            point, and a key in ``LastEvaluatedKey`` to apply in a subsequent
            operation, so that you can pick up where you left off. Also, if the
            processed data set size exceeds 1 MB before DynamoDB reaches this
            limit, it stops the operation and returns the matching values up to
            the limit, and a key in ``LastEvaluatedKey`` to apply in a
            subsequent operation to continue the operation. For more
            information, see `Query and Scan <http://docs.aws.amazon.com/amazo
            ndynamodb/latest/developerguide/QueryAndScan.html>`_ in the Amazon
            DynamoDB Developer Guide.
        :param str return_consumed_capacity: Determines the level of detail
            about provisioned throughput consumption that is returned in the
            response. Should be ``None`` or one of ``INDEXES`` or ``TOTAL``
        :param int segment: For a parallel *Scan* request, ``segment``
            identifies an individual segment to be scanned by an application
            worker.

            Segment IDs are zero-based, so the first segment is always ``0``.
            For example, if you want to use four application threads to scan a
            table or an index, then the first thread specifies a Segment value
            of ``0``, the second thread specifies ``1``, and so on.

            The value of ``LastEvaluatedKey`` returned from a parallel *Scan*
            request must be used as ``ExclusiveStartKey`` with the same segment
            ID in a subsequent *Scan* operation.

            The value for ``segment`` must be greater than or equal to ``0``,
            and less than the value provided for ``total_segments``.

            If you provide ``segment``, you must also provide
            ``total_segments``.
        :param int total_segments: For a parallel *Scan* request,
            ``total_segments`` represents the total number of segments into
            which the *Scan* operation will be divided. The value of
            ``total_segments`` corresponds to the number of application workers
            that will perform the parallel scan. For example, if you want to
            use four application threads to scan a table or an index, specify a
            ``total_segments`` value of 4.

            The value for ``total_segments`` must be greater than or equal to
            ``1``, and less than or equal to ``1000000``. If you specify a
            ``total_segments`` value of ``1``, the *Scan* operation will be
            sequential rather than parallel.

            If you specify ``total_segments``, you must also specify
            ``segments``.
        :rtype: dict

        :raises: :exc:`~sprockets.clients.dynamodb.exceptions.DynamoDBException`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ConfigNotFound`
                 :exc:`~sprockets.clients.dynamodb.exceptions.NoCredentialsError`
                 :exc:`~sprockets.clients.dynamodb.exceptions.NoProfileError`
                 :exc:`~sprockets.clients.dynamodb.exceptions.TimeoutException`
                 :exc:`~sprockets.clients.dynamodb.exceptions.RequestException`
                 :exc:`~sprockets.clients.dynamodb.exceptions.InternalFailure`
                 :exc:`~sprockets.clients.dynamodb.exceptions.LimitExceeded`
                 :exc:`~sprockets.clients.dynamodb.exceptions.MissingParameter`
                 :exc:`~sprockets.clients.dynamodb.exceptions.OptInRequired`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ResourceInUse`
                 :exc:`~sprockets.clients.dynamodb.exceptions.RequestExpired`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ResourceNotFound`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ServiceUnavailable`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ThroughputExceeded`
                 :exc:`~sprockets.clients.dynamodb.exceptions.ValidationException`

        .. _Scan: http://docs.aws.amazon.com/amazondynamodb/
           latest/APIReference/API_Scan.html

        """
        raise NotImplementedError

    @staticmethod
    def _process_response(response):
        error = response.exception()
        if error:
            if isinstance(error, aws_exceptions.AWSError):
                if error.args[1]['type'] in exceptions.MAP:
                    raise exceptions.MAP[error.args[1]['type']](
                        error.args[1]['message'])
            raise error
        http_response = response.result()
        if not http_response or not http_response.body:
            raise exceptions.DynamoDBException('empty response')
        return json.loads(http_response.body.decode('utf-8'))


def _unwrap_result(function, result):
    if result:
        if function == 'GetItem':
            return utils.unmarshall(result['Item'])
        if function == 'Query':
            return [utils.unmarshall(item) for item in result['Items']]
    return result
