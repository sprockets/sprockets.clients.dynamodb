"""
DynamoDB Exceptions
===================

"""


class DynamoDBException(Exception):
    """Base exception that is extended by all exceptions raised by
    tornado_dynamodb.

    :ivar msg: The error message

    """

    def __init__(self, *args, **kwargs):
        super(DynamoDBException, self).__init__(*args, **kwargs)


class RequestException(DynamoDBException):
    """Raised when the HTTP request failed due to a network or DNS related
    issue.

    """
    pass


class ConditionalCheckFailedException(DynamoDBException):
    """A condition specified in the operation could not be evaluated."""
    pass


class ConfigNotFound(DynamoDBException):
    """The configuration file could not be parsed."""
    pass


class ConfigParserError(DynamoDBException):
    """Error raised when parsing a configuration file with
    :class:`~configparser.RawConfigParser`

    """
    pass


class InternalFailure(DynamoDBException):
    """The request processing has failed because of an unknown error, exception
    or failure.

    """
    pass


class ItemCollectionSizeLimitExceeded(DynamoDBException):
    """An item collection is too large. This exception is only returned for
    tables that have one or more local secondary indexes.

    """
    pass


class InvalidAction(DynamoDBException):
    """The action or operation requested is invalid. Verify that the action is
    typed correctly.

    """
    pass


class InvalidParameterCombination(DynamoDBException):
    """Parameters that must not be used together were used together."""
    pass


class InvalidParameterValue(DynamoDBException):
    """An invalid or out-of-range value was supplied for the input parameter."""
    pass


class InvalidQueryParameter(DynamoDBException):
    """The AWS query string is malformed or does not adhere to AWS standards."""
    pass


class LimitExceeded(DynamoDBException):
    """The number of concurrent table requests (cumulative number of tables in
    the ``CREATING``, ``DELETING`` or ``UPDATING`` state) exceeds the maximum
    allowed of ``10``.

    Also, for tables with secondary indexes, only one of those tables can be in
    the ``CREATING`` state at any point in time. Do not attempt to create more
    than one such table simultaneously.

    The total limit of tables in the ``ACTIVE`` state is ``250``.

    """
    pass


class MalformedQueryString(DynamoDBException):
    """The query string contains a syntax error."""
    pass


class MissingParameter(DynamoDBException):
    """A required parameter for the specified action is not supplied."""
    pass


class NoCredentialsError(DynamoDBException):
    """Raised when the credentials could not be located."""
    pass


class NoProfileError(DynamoDBException):
    """Raised when the specified profile could not be located."""
    pass


class OptInRequired(DynamoDBException):
    """The AWS access key ID needs a subscription for the service."""
    pass


class ThroughputExceeded(DynamoDBException):
    """Your request rate is too high. The AWS SDKs for DynamoDB automatically
    retry requests that receive this exception. Your request is eventually
    successful, unless your retry queue is too large to finish. Reduce the
    frequency of requests and use exponential backoff. For more information, go
    to `Error Retries and Exponential Backoff <http://docs.aws.amazon.com/
    amazondynamodb/latest/developerguide/ErrorHandling.html#APIRetries>`_ in
    the Amazon DynamoDB Developer Guide.

    """
    pass


class RequestException(DynamoDBException):
    """A generic HTTP request exception has occurred when communicating with
    DynamoDB.

    """
    pass


class RequestExpired(DynamoDBException):
    """The request reached the service more than 15 minutes after the date
    stamp on the request or more than 15 minutes after the request expiration
    date (such as for pre-signed URLs), or the date stamp on the request is
    more than 15 minutes in the future.

    """
    pass


class ResourceInUse(DynamoDBException):
    """he operation conflicts with the resource's availability. For example,
    you attempted to recreate an existing table, or tried to delete a table
    currently in the ``CREATING`` state.

    """
    pass


class ResourceNotFound(DynamoDBException):
    """The operation tried to access a nonexistent table or index. The resource
    might not be specified correctly, or its status might not be ``ACTIVE``.

    """
    pass


class ServiceUnavailable(DynamoDBException):
    """The request has failed due to a temporary failure of the server."""
    pass


class ThrottlingException(DynamoDBException):
    """This exception might be returned if the following API operations are
    requested too rapidly: CreateTable; UpdateTable; DeleteTable.

    """
    pass


class TimeoutException(DynamoDBException):
    """The request to DynamoDB timed out."""
    pass


class ValidationException(DynamoDBException):
    """The input fails to satisfy the constraints specified by an AWS service.
    This error can occur for several reasons, such as a required parameter
    that is missing, a value that is out range, or mismatched data types. The
    error message contains details about the specific part of the request that
    caused the error.

    """
    pass


MAP = {
    'com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException':
    ConditionalCheckFailedException,
    'com.amazonaws.dynamodb.v20120810#ItemCollectionSizeLimitExceededException':
    ItemCollectionSizeLimitExceeded,
    'com.amazonaws.dynamodb.v20120810#InternalFailure': InternalFailure,
    'com.amazonaws.dynamodb.v20120810#LimitExceededException': LimitExceeded,
    'com.amazonaws.dynamodb.v20120810#ProvisionedThroughputExceededException':
    ThroughputExceeded,
    'com.amazonaws.dynamodb.v20120810#ResourceInUseException': ResourceInUse,
    'com.amazonaws.dynamodb.v20120810#ResourceNotFoundException':
    ResourceNotFound,
    'com.amazonaws.dynamodb.v20120810#ThrottlingException':
    ThrottlingException,
    'com.amazon.coral.validate#ValidationException': ValidationException
}
