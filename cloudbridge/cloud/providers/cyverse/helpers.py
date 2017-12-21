"""
A set of Cyverse-specific helper methods used by the framework.
"""


def config_whitelist(config, whitelist):
    result = {}
    for key in config:
        if key in whitelist:
            result[key] = config[key]
    return result
