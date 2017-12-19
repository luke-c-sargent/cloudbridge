"""
A set of Cyverse-specific helper methods used by the framework.
"""
from .provider import CyverseCloudProvider


def config_whitelist(config):
    result = {}
    for key in config:
        if key in CyverseCloudProvider.API_VARS:
            result[key] = config[key]
    return result
