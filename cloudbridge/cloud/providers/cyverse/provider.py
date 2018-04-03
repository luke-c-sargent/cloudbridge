from os import environ

from cloudbridge.cloud.base import BaseCloudProvider

from .helpers import config_whitelist
from .resources import CyverseConnection
from .services import CyverseStorageService


class CyverseCloudProvider(BaseCloudProvider):
    '''Cyverse provider interface'''

    PROVIDER_ID = 'cyverse'
    CYVERSE_DEFAULT_HOST = 'data.iplantcollaborative.org'
    CYVERSE_DEFAULT_ZONE = 'iplant'
    CYVERSE_DEFAULT_PORT = '1247'
    CYVERSE_DEFAULT_API_SERVER = 'agave.iplantc.org'
    CYVERSE_SYSTEM_ID = CYVERSE_DEFAULT_HOST

    # takes config, a dict of configuration values
    def __init__(self, config):
        #print("Input config:\n{}\n".format(config))
        super(CyverseCloudProvider, self).__init__(
            config_whitelist(config, CyverseConnection.API_VARS))
        # initialize services
        self._conn = CyverseConnection(self)
        self._storage = CyverseStorageService(self)

    def _get_config_value(self, key):
        result = super(
            CyverseCloudProvider, self)._get_config_value(key, None)
        if not result:
            result = environ.get(
                'CYVERSE_ENV_{0}'.format(key.upper()), None)
            if not result and hasattr(
                self, "CYVERSE_DEFAULT_{0}".format(key.upper())):
                result = getattr(self, "CYVERSE_DEFAULT_{0}".format(key.upper()))
        return result

    @property
    def compute(self):
        raise NotImplementedError(
            "Cyverse does not implement this service")

    @property
    def networking(self):
        raise NotImplementedError(
            "Cyverse does not implement this service")

    @property
    def security(self):
        raise NotImplementedError(
            "Cyverse does not implement this service")

    @property
    def storage(self):
        return self._storage
