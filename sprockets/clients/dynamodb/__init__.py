try:
    from .connector import DynamoDB
except ImportError as error:
    def DynamoDB(*args, **kwargs):
        raise error

version_info = (0, 2, 2)
__version__ = '.'.join(str(v) for v in version_info)

# Response constants
TABLE_ACTIVE = 'ACTIVE'
TABLE_CREATING = 'CREATING'
TABLE_DELETING = 'DELETING'
TABLE_DISABLED = 'DISABLED'
TABLE_UPDATING = 'UPDATING'

