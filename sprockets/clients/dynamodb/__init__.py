try:
    from .connector import DynamoDB
except ImportError as error:
    def DynamoDB(*args, **kwargs):
        raise error

version_info = (0, 0, 0)
__version__ = '.'.join(str(v) for v in version_info)
__all__ = ['DynamoDB', 'version_info', '__version__']
