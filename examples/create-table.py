"""
Create the table described in `CreateTable`_.

This example creates a table if it does not exist using chained
callbacks.

.. _CreateTable: http://docs.aws.amazon.com/amazondynamodb/latest/
   APIReference/API_CreateTable.html

"""
import logging
import sys

from sprockets.clients import dynamodb
from tornado import ioloop


LOGGER = logging.getLogger('create-database')
TABLE_DEF = {
    'TableName': 'Thread',
    'AttributeDefinitions': [
        {'AttributeName': 'ForumName', 'AttributeType': 'S'},
        {'AttributeName': 'Subject', 'AttributeType': 'S'},
        {'AttributeName': 'LastPostDateTime', 'AttributeType': 'S'},
    ],
    'KeySchema': [
        {'AttributeName': 'ForumName', 'KeyType': 'HASH'},
        {'AttributeName': 'Subject', 'KeyType': 'RANGE'},
    ],
    'LocalSecondaryIndexes': [
        {
            'IndexName': 'LastPostIndex',
            'KeySchema': [
                {'AttributeName': 'ForumName', 'KeyType': 'HASH'},
                {'AttributeName': 'LastPostDateTime', 'KeyType': 'RANGE'},
            ],
            'Projection': {
                'ProjectionType': 'KEYS_ONLY',
            },
        },
    ],
    'ProvisionedThroughput': {
        'ReadCapacityUnits': 5,
        'WriteCapacityUnits': 5,
    }
}

dynamo = dynamodb.DynamoDB()
iol = ioloop.IOLoop.instance()


def on_table_described(describe_response):

    def on_created(create_response):
        try:
            result = create_response.result()
            LOGGER.info('table created - %r', result)
            iol.add_callback(iol.stop)
        except Exception:
            LOGGER.exception('failed to create table')
            sys.exit(-1)

    try:
        result = describe_response.result()
        LOGGER.info('found table %s, created %s',
                    result['Table']['TableName'],
                    result['Table']['CreationDateTime'])
        iol.add_callback(iol.stop)
    except Exception as error:
        LOGGER.warn('table not found, attempting to create: %s', error)
        next_future = dynamo.create_table(TABLE_DEF)
        iol.add_future(next_future, on_created)

logging.basicConfig(level=logging.DEBUG,
                    format='%(levelname)1.1s %(name)s: %(message)s')

iol.add_future(dynamo.describe_table(TABLE_DEF['TableName']),
               on_table_described)
iol.start()
