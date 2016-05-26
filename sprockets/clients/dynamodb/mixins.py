import logging

from sprockets.clients.dynamodb import exceptions
from tornado import gen, web


class DynamoMixin(web.RequestHandler):

    def initialize(self):
        super(DynamoMixin, self).initialize()
        dynamocfg = self.settings['dynamodb']
        self.__max_retries = int(dynamocfg.get('max_retries', '3'))
        self.__rate_limit_retry = int(dynamocfg.get('retry_after', '3'))
        self.dynamo = self.application.dynamo
        if not hasattr(self, 'logger'):
            self.logger = logging.getLogger(self.__class__.__name__)

    def _on_dynamodb_exception(self, error, attempt):
        """
        :param exceptions.DynamoDBException error:
        """
        if isinstance(error, exceptions.NoCredentialsError):
            self.logger.error('DynamoDB credentials error: %s', error)
            self.send_error(500, reason='Database Authentication')
        elif isinstance(error, exceptions.RequestException):
            if attempt >= self.__max_retries:
                self.send_error(500, reason='Database Request Failure')
            else:
                self.logger.warning('DynamoDB request exception #%i, '
                                    'retrying', attempt)
                return
        elif isinstance(error, exceptions.ThroughputExceeded):
            self.logger.warning('DynamoDB throughput exceeded: %s', error)
            self.set_status(429)
            self.add_header('Retry-After', self.__rate_limit_retry)
            if hasattr(self, 'send_response'):
                self.send_response(
                    {'error': 'Database throughput exceeded. Retry after {} '
                              'seconds'.format(self.__rate_limit_retry)})
            self.finish()
        elif isinstance(error, exceptions.TimeoutException):
            self.logger.error('DynamoDB timeout')
            self.send_error(500, reason='Database Timeout')
        else:
            self.logger.error('DynamoDB error: %s (%r)', error, error.args)
            self.send_error(500, reason='Unexpected Error {}'.format(error))

        raise error

    @gen.coroutine
    def get_item(self, table_name, key):
        for attempt in range(0, self.__max_retries + 1):
            try:
                self.logger.debug('get_item %d', attempt)
                item = yield self.dynamo.get_item(table_name, key)
                raise gen.Return(item)
            except exceptions.DynamoDBException as error:
                self._on_dynamodb_exception(error, attempt)

    @gen.coroutine
    def put_item(self, table_name, item):
        for attempt in range(0, self.__max_retries + 1):
            try:
                result = yield self.dynamo.put_item(table_name, item)
                raise gen.Return(result)
            except exceptions.DynamoDBException as error:
                self._on_dynamodb_exception(error, attempt)

    @gen.coroutine
    def dynamodb_execute(self, function, arg_dict):
        for attempt in range(0, self.__max_retries + 1):
            try:
                results = yield self.dynamo.execute(function, arg_dict)
                raise gen.Return(results)
            except exceptions.DynamoDBException as error:
                self._on_dynamodb_exception(error, attempt)
