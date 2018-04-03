"""
DataTypes used by this provider
"""
from agavepy.agave import Agave

from cloudbridge.cloud.base.resources import BaseBucket
from cloudbridge.cloud.base.resources import BaseBucketContainer
from cloudbridge.cloud.base.resources import BaseBucketObject
from cloudbridge.cloud.base.resources import ClientPagedResultList
from cloudbridge.cloud.interfaces.exceptions \
    import InvalidConfigurationException


class CyverseClient(object):
    """
    Cyverse client definition
    """
    PARAMETERS = ["client_name", "callback_url", "description"]

    DEFAULT_NAME = "CloudBridge"
    DEFAULT_DESCRIPTION = "Cyverse access via AgaveAPI"
    DEFAULT_CALLBACKURL = ""

    # arguments are strings
    def __init__(self,
                 clientName=DEFAULT_NAME,
                 callback_url=DEFAULT_CALLBACKURL,
                 description=DEFAULT_DESCRIPTION):
        self.clientName = clientName
        self.description = description
        self.callback_url = callback_url

    @property
    def client_name(self):
        return self.clientName

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
        'api_server', 'user', 'pass', 'api_key',
        'api_secret', 'token', 'refresh_token', 'client_name'
    ]
    SYNONYMS = {
        "user": "username",
        "pass": "password",
        "client_name": "clientName",
        "consumerSecret": "api_secret",
        "consumerKey": "api_key"
    }

    # initialize the Agave API and client objects
    def __init__(self, provider):
        # Create configuration from inputted config
        def config_keys(config, synonyms=self.SYNONYMS):
            result = {}
            for key in config:
                if key is not None:
                    if synonyms is not None and key in synonyms:
                        gotten_value = provider._get_config_value(key)
                        # print("GV: {}".format(gotten_value))
                        if gotten_value is not None:
                            result[synonyms[key]] = gotten_value
                    else:
                        gotten_value = provider._get_config_value(key)
                        if gotten_value is not None:
                            result[key] = gotten_value
            return result
        keys = config_keys(CyverseConnection.API_VARS)
        # print("The api is getting:\n{}".format(keys))
        self._api = Agave(**keys)
        self._client = CyverseClient(**config_keys(CyverseClient.PARAMETERS,
                                                   CyverseConnection.SYNONYMS))

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
                raise InvalidConfigurationException(
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
        self._bucket = provider._get_config_value("username")
        self._object_container = CyverseBucketContainer(provider, self)

    @property
    def id(self):
        return self._bucket

    @property
    def name(self):
        return self._bucket

    @property
    def objects(self):
        return self._object_container.list().data


class CyverseBucketContainer(BaseBucketContainer):
    def __init__(self, provider, bucket):
        self._file_handle = provider._conn._api.files
        super(CyverseBucketContainer, self).__init__(provider, bucket)
        self.details = {"filePath": provider._get_config_value("user"),
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
    FILE_INFO = ["format", "_links", "system", "lastModified", "permissions",
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
        self.systemId = provider.CYVERSE_SYSTEM_ID
        self.filePath = "/{}/".format(provider._get_config_value("user"))
        self._info = obj

    def iter_content(self):
        data = self._provider._conn.files.download(
            filePath=self.filePath, systemId=self.systemId).content
        return self.BucketObjIterator(data)

    def delete(self):
        print(self._provider._conn.files.delete(
            filePath=self.filePath, systemId=self.systemId))

    def upload(self, data):
        pass

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

    def upload_from_file(self, path):
        info = None
        with open(path, "rb") as f:
            info = self._provider._conn.files.importData(fileToUpload=f,
                                                         **self.FILE_CONF)
        return info
