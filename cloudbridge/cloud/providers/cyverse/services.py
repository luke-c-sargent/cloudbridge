"""
Services implemented by the Cyverse provider
"""
from cloudbridge.cloud.base.services import BaseBucketService
from cloudbridge.cloud.base.services import BaseStorageService

from .resources import CyverseBucket


class CyverseStorageService(BaseStorageService):

    def __init__(self, provider):
        super(CyverseStorageService, self).__init__(provider)
        self._bucket_svc = CyverseBucketService(provider)

    @property
    def buckets(self):
        return self._bucket_svc

    @property
    def volumes(self):
        raise NotImplementedError(
            "Cyverse does not implement this service")

    @property
    def snapshots(self):
        raise NotImplementedError(
            "Cyverse does not implement this service")


# cyverse does not have multiple buckets
class CyverseBucketService(BaseBucketService):

    def __init__(self, provider):
        super(CyverseBucketService, self).__init__(provider)
#       since there is one 'bucket,' the only unique ID required is username
        self._bucket = CyverseBucket(provider)

#   bucket ID = username in cyverse
    def get(self):
        return self._bucket

    def find(self, name):
        raise NotImplementedError(
            "cannot find: Cyverse does not implement multiple 'buckets'")

    def list(self):
        raise NotImplementedError(
            "cannot list: Cyverse has only one 'bucket' per user")

    def create(self, name):
        raise NotImplementedError(
            "cannot create: Cyverse has only one 'bucket' per user")
