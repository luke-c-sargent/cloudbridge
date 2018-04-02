import sys
import traceback
from contextlib import contextmanager

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization as crypt_serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from six import reraise


def generate_key_pair():
    """
    This method generates a keypair and returns it as a tuple
    of (public, private) keys.
    The public key format is OpenSSH and private key format is PEM.
    """
    key_pair = rsa.generate_private_key(
        backend=default_backend(),
        public_exponent=65537,
        key_size=2048)
    private_key = key_pair.private_bytes(
        crypt_serialization.Encoding.PEM,
        crypt_serialization.PrivateFormat.PKCS8,
        crypt_serialization.NoEncryption()).decode('utf-8')
    public_key = key_pair.public_key().public_bytes(
        crypt_serialization.Encoding.OpenSSH,
        crypt_serialization.PublicFormat.OpenSSH).decode('utf-8')
    return public_key, private_key


def filter_by(prop_name, kwargs, objs):
    """
    Utility method for filtering a list of objects by a property.
    If the given property has a non empty value in kwargs, then
    the list of objs is filtered by that value. Otherwise, the
    list of objs is returned as is.
    """
    prop_val = kwargs.pop(prop_name, None)
    if prop_val:
        match = (o for o in objs if getattr(o, prop_name) == prop_val)
        return match
    return objs


def generic_find(filter_names, kwargs, objs):
    """
    Utility method for filtering a list of objects by a list of filters.
    """
    matches = objs
    for name in filter_names:
        matches = filter_by(name, kwargs, matches)

    # All kwargs should have been popped at this time.
    if len(kwargs) > 0:
        raise TypeError(
            "Unrecognised parameters for search: %s. Supported attributes: %s"
            % (kwargs, filter_names))

    return matches


@contextmanager
def cleanup_action(cleanup_func):
    """
    Context manager to carry out a given
    cleanup action after carrying out a set
    of tasks, or when an exception occurs.
    If any errors occur during the cleanup
    action, those are ignored, and the original
    traceback is preserved.

    :params func: This function is called if
    an exception occurs or at the end of the
    context block. If any exceptions raised
        by func are ignored.
    Usage:
        with cleanup_action(lambda e: print("Oops!")):
            do_something()
    """
    try:
        yield
    except Exception:
        ex_class, ex_val, ex_traceback = sys.exc_info()
        try:
            cleanup_func()
        except Exception as e:
            print("Error during exception cleanup: {0}".format(e))
            traceback.print_exc()
        reraise(ex_class, ex_val, ex_traceback)
    try:
        cleanup_func()
    except Exception as e:
        print("Error during cleanup: {0}".format(e))
        traceback.print_exc()
