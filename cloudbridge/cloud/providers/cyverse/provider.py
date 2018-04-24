from os import environ

from cloudbridge.cloud.base import BaseCloudProvider

from .helpers import config_whitelist
from .resources import CyverseClient
from .resources import CyverseConnection
from .services import CyverseStorageService


class CyverseCloudProvider(BaseCloudProvider):
    '''Cyverse provider interface'''

    PROVIDER_ID = 'cyverse'
    CYVERSE_DEFAULT_HOST = 'data.iplantcollaborative.org'
    CYVERSE_DEFAULT_ZONE = 'iplant'
    CYVERSE_DEFAULT_PORT = '1247'
    CYVERSE_DEFAULT_API_SERVER = 'https://agave.iplantc.org'
    CYVERSE_DEFAULT_CLIENTNAME = CyverseClient.DEFAULT_NAME
    CYVERSE_DEFAULT_FILEPATH = "/"  # the home directory of a user's store
    CYVERSE_SYSTEM_ID = CYVERSE_DEFAULT_HOST

    # takes config, a dict of configuration values
    def __init__(self, config):
        super(CyverseCloudProvider, self).__init__(
            config_whitelist(config, CyverseConnection.API_VARS))
        # initialize services
        self._conn = CyverseConnection(self)
        self._storage = CyverseStorageService(self)

    # configuration ingestion preference
    # 1. provider config object
    # 2. environment variable
    #   a. if file_path hasn't been found, check if user name
    # 3. defaults
    def _get_config_value(self, key):
        # check passed in config
        result = super(
            CyverseCloudProvider, self)._get_config_value(key, None)
        if not result:
            # check env
            result = environ.get(
                'CYVERSE_ENV_{0}'.format(key.upper()), None)
            # check built-in defaults
            if not result and hasattr(
                        self, "CYVERSE_DEFAULT_{0}".format(key.upper())):
                result = getattr(self,
                                 "CYVERSE_DEFAULT_{0}".format(key.upper()))
        return result

    @property
    def compute(self):
        raise NotImplementedError(
            "This service not yet implemented")

    @property
    def networking(self):
        raise NotImplementedError(
            "This service not yet implemented")

    @property
    def security(self):
        raise NotImplementedError(
            "This service not yet implemented")

    @property
    def storage(self):
        return self._storage
