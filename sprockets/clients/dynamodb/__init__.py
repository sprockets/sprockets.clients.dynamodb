try:
    from .connector import DynamoDB
except ImportError as error:
    def DynamoDB(*args, **kwargs):
        raise error

version_info = (0, 0, 0)
__version__ = '.'.join(str(v) for v in version_info)

# Response constants
TABLE_ACTIVE = 'ACTIVE'
TABLE_CREATING = 'CREATING'
TABLE_DELETING = 'DELETING'
TABLE_DISABLED = 'DISABLED'
TABLE_UPDATING = 'UPDATING'

# Table stream view type constants
STREAM_VIEW_NEW_IMAGE = 'NEW_IMAGE'
STREAM_VIEW_OLD_IMAGE = 'OLD_IMAGE'
STREAM_VIEW_NEW_AND_OLD_IMAGES = 'NEW_AND_OLD_IMAGES'
STREAM_VIEW_KEYS_ONLY = 'KEYS_ONLY'
_STREAM_VIEW_TYPES = (STREAM_VIEW_NEW_IMAGE, STREAM_VIEW_OLD_IMAGE,
                      STREAM_VIEW_NEW_AND_OLD_IMAGES, STREAM_VIEW_KEYS_ONLY)
