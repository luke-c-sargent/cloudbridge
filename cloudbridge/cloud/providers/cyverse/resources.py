"""
DataTypes used by this provider
"""
from agavepy.agave import Agave

from cloudbridge.cloud.providers.cyverse import CyverseCloudProvider
from cloudbridge.cloud.base.resources import BaseBucket
from cloudbridge.cloud.base.resources import BaseBucketContainer
from cloudbridge.cloud.base.resources import BaseBucketObject
from cloudbridge.cloud.base.resources import ClientPagedResultList
from cloudbridge.cloud.interfaces.exceptions import CloudBridgeError


class CyverseClient(object):
    """
    Cyverse client definition
    """
    PARAMETERS = ["clientName", "callback_url", "description"]

    DEFAULT_NAME = "CloudBridge"
    DEFAULT_DESCRIPTION = "Cyverse access via AgaveAPI"
    DEFAULT_CALLBACKURL = ""

    # arguments are strings
    def __init__(self,
                 name=DEFAULT_NAME,
                 callback_url=DEFAULT_CALLBACKURL,
                 description=DEFAULT_DESCRIPTION):
        self.clientName = name if name is not None else self.DEFAULT_NAME
        self.description = description
        self.callback_url = callback_url

    def __eq__(self, other):
        return reduce(
            (lambda x, y: x and y),
            [self.x == other.x for x in self.PARAMETERS])

    def keys(self):
        return [x for x in self.PARAMETERS if hasattr(self, x)]


class CyverseConnection(object):
    # these client parameters have defaults
    # the presence of these determines client checking / creating
    # key: the AgavePy name     value: the API name
    API_VARS = [
        'username', 'password',
        'api_key', 'api_secret',
        'token', 'refresh_token'
    ]
    SYNONYMS = {
        "user": "username",
        "pass": "password",
        "consumerSecret": "api_secret",
        "consumerKey": "api_key"
    }

    # initialize the Agave API and client objects
    def __init__(self, provider):
        # Create configuration from inputted config
        def config_keys(self, config):
            result = {}
            for key in config:
                if key is not None:
                    result[key] = self._get_config_value(key)
            return result

        self._api = Agave(**config_keys(CyverseCloudProvider.API_VARS))
        self._client = CyverseClient(**config_keys(CyverseClient.PARAMETERS))

    # check if provided client app is in user's client app list
    def check_clients(self):
        for client in self._api.clients.list():
            if client.name == self._client.clientName:
                return True
        else:
            return False

    def create_client(self, overwrite=True):
        client = self._client
        if self.check_clients():
            if overwrite:
                self.delete_client()
            else:
                raise CloudBridgeError(
                    "Client already exists and overwrite set to False")
        if type(client) is not str:
            client = client.clientName
            _body = {"clientName": client}
        else:
            # a list of parameters that the client creation accepts
            _body = {key: getattr(client, key) for key in client.keys()}
        try:
            result = self._api.clients.create(body=_body)
            return {"api_key": result["consumerKey"],
                    "api_secret": result["consumerSecret"]}
        except KeyboardInterrupt:
            raise

    def delete_client(self):
        client = self._client
        if not self.check_clients():
            print("Client {} is not in client list".format(client.name))
        # if client name is string
        if type(client) is str:
            client_name = client
        else:
            client_name = client.clientName
        self._api.clients.delete(clientName=client_name)


class CyverseBucket(BaseBucket):
    def __init__(self, provider):
        super(CyverseBucket, self).__init__(provider)
        self._bucket = provider.username
        self._object_container = CyverseBucketContainer(provider, self)

    @property
    def id(self):
        return self._bucket

    @property
    def name(self):
        return self._bucket

    @property
    def objects(self):
        return self._object_container


class CyverseBucketContainer(BaseBucketContainer):
    def __init__(self, provider, bucket):
        self._file_handle = provider._conn._api.files
        super(CyverseBucketContainer, self).__init__(provider, bucket)
        self.details = {"filePath": provider.username,
                        "systemId": self._provider.CYVERSE_SYSTEM_ID}

    def _get_files(self):
        pass

    def get(self, name):
        pass
#        try:
#            # pylint:disable=protected-access
#            obj = self.bucket._bucket.Object(name)

    def list(self, limit=None, marker=None):
        results = self._file_handle.list(**self.details)
        objects = [CyverseBucketObject(self._provider, _obj)
                   for _obj in results]
        return ClientPagedResultList(
            self._provider, objects, limit=limit, marker=marker)

    def find(self, name, limit=None):
        return self.list()


class CyverseBucketObject(BaseBucketObject):
    PARAMS = ["format", "_links", "system", "lastModified", "permissions",
              "path", "name", "mimeType", "type", "length"]

    class BucketObjIterator():
        CHUNK_SIZE = 4096

        def __init__(self, body):
            self.idx = 0
            self.body = body

        def __iter__(self):
            while True:
                data = self.read(self.CHUNK_SIZE)
                if data:
                    yield data
                else:
                    self.close()
                    break

        def read(self, length):
            base = self.idx*length
            self.idx += 1
            return self.body[base:base + length]

        def close(self):
            self.idx = 0

    def __init__(self, provider, obj):
        super(CyverseBucketObject, self).__init__(provider)
        self.FILE_CONF["systemId"] = CyverseCloudProvider.CYVERSE_SYSTEM_ID
        self.FILE_CONF["filePath"] = self._provider.username
        self._object = obj

    def iter_content(self):
        data = self._provider._conn.files.download(
            filePath=self.path, systemId=self.system).content
        return self.BucketObjIterator(data)

    # path is unique; two files can't exist in the same place
    @property
    def id(self):
        return self.path

    @property
    def format(self):
        return self._info["format"]

    @property
    def links(self):
        return self._info["_links"]

    @property
    def system(self):
        return self._info["system"]

    @property
    def last_modified(self):
        return self._info["lastModified"]

    @property
    def permissions(self):
        return self._info["permissions"]

    @property
    def path(self):
        return self._info["path"]

    @property
    def name(self):
        return self._info["name"]

    @property
    def mimeType(self):
        return self._info["mimeType"]

    @property
    def type(self):
        return self._info["type"]

    @property
    def size(self):
        return self._info["length"]

    def upload(self, data):
        with open(filename, "rb") as f:
            imp_conf["fileToUpload"] = f
            handle = conn._api.files.importData(**imp_conf)

    def upload_from_file(self, path):
        pass
