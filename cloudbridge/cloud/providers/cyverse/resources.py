"""
DataTypes used by this provider
"""
from os.path import isdir, abspath
from os import walk
from sys import exit

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
    PARAMETERS = ["clientName", "callbackUrl", "description"]
    OPTIONAL_PARAMETERS = ["consumerKey", "consumerSecret"]
    DEFAULT_NAME = "CloudBridge"
    DEFAULT_DESCRIPTION = "Cyverse access via AgaveAPI"
    DEFAULT_CALLBACKURL = ""

    SYNONYMS = {
        "name" : "clientName"
    }

    # arguments are strings
    def __init__(self,
                 clientName=DEFAULT_NAME,
                 callbackUrl=DEFAULT_CALLBACKURL,
                 description=DEFAULT_DESCRIPTION,
                 consumerKey=None,
                 consumerSecret=None,
                 **kwargs):
        if "name" in kwargs:
            self.clientName = kwargs["name"]
            del kwargs["name"]
        else:
            self.clientName = clientName
        self.description = description
        self.callbackUrl = callbackUrl
        self.consumerKey = consumerKey
        self.consumerSecret = consumerSecret
        # extraneous arguments
        self.extraneousParameters = kwargs
        print("Extraneous params:{}".format(kwargs))

        # consumer_key/secret if this information is available

    def __repr__(self, show_secret=False):
        extraneous = ""
        if self.extraneousParameters:
            extraneous = "\n\t- extra info:: "
            for e in self.extraneousParameters:
                extraneous = extraneous + "{} : {}, ".format(e, self.extraneousParameters[e])
            extraneous = extraneous + "\n|CyverseClient"
        if show_secret:
            secret = self.consumerSecret
        else:
            secret = "<redacted>"
        return "<CyverseClient| name: {}, callback: {}, description: {} consumerKey: {} consumerSecret: {} {}/>".format(
            self.clientName, self.callbackUrl, self.description, self.consumerKey, secret, extraneous)

    @property
    def client_name(self):
        return self.clientName

    # we can't access the remote client consumerSecret without remaking,
    # but if the consumerKey is the same, the secret's the same too
    def __eq__(self, other):
        return reduce(
            (lambda x, y: x and y),
                [self.x == other.x for x in self.PARAMETERS] +
                [self.consumerKey == other.consumerKey]
            )

    def keys(self, extraneous=False):
        extras = []
        if extraneous:
            for e in self.extraneousParameters:
                extras = extras + [e]
        return [x for x in self.PARAMETERS + self.OPTIONAL_PARAMETERS + extras
            if hasattr(self, x)]


class CyverseConnection(object):
    # these client parameters have defaults
    # the presence of these determines client checking / creating
    # key: the AgavePy name     value: the API name

    # connection steps:
    # 1. just user/pass/api server?
    #  a. create client, get api_keys, get tokens
    # 2.w

    API_VARS = [
        'api_server', 'username', 'password', 'consumerKey', 'consumerSecret',
        'token', 'refresh_token', 'clientName', 'systemId', 'callbackUrl'
    ]

    # may not be needed anymore
    # keys:value = atypical name : required name
    SYNONYMS = {
        "user": "username",
        "pass": "password",
        "client_name": "clientName",
        "api_secret" : "consumerSecret",
        "api_key" : "consumerKey"
    }

    # initialize the Agave API and client objects
    def __init__(self, provider):
        # Create configuration from inputted config
        def config_keys(config, synonyms=self.SYNONYMS):
            result = {}
            #print("CFG: {}".format(config))
            for key in config:
                #if key is not None:
                if key in synonyms:
                    gotten_value = provider._get_config_value(key)
                    #print("IK: {} || GV: {}".format(key,gotten_value))
                    if gotten_value is not None:
                        #print("??{}??".format(gotten_value))
                        result[synonyms[key]] = gotten_value
                else:
                    gotten_value = provider._get_config_value(key)
                    #print("EK: {} || GV: {}".format(key,gotten_value))
                    if gotten_value is not None:
                        #print("??{}??".format(gotten_value))
                        result[key] = gotten_value
            #print("R:{}".format(result))
            return result

        keys = config_keys(CyverseConnection.API_VARS)
        print("The api is getting:\n{}".format(keys))
        self._api = Agave(**keys)
        # get provided client info
        ckeys = config_keys(CyverseClient.PARAMETERS +
            CyverseClient.OPTIONAL_PARAMETERS)
        print("The client is getting:\n{}".format(ckeys))
        self._client = CyverseClient(**ckeys)
        # check it against remote client list to ensure our info is correct
        self._verify_client()
        print(self)

    def __repr__(self):
        return "<CyverseConnection|\n - {}\n|CyverseConnection\>".format(str(self._client))

    def create_client(self, overwrite=True):
        client = self._client
        if self.check_clients():
            # only overwrites if consumerSecret or consumerKey are missing,
            # in order to populate these values
            if overwrite:
                self.delete_client()
            # if it's in the list and we don't need to overwrite, we're done
            else:
                return
        #if type(client) is not str:
        #    client = client.clientName
        #    _body = {"clientName": client}

        # if the clients are not equal:

        # a list of parameters that the client creation accepts
        _body = {key: getattr(client, key) for key in client.keys() if getattr(client,key) is not None }
        try:
            result = self._api.clients.create(body=_body)
            #print("88{}: {}".format(type(result), result))
            self._client = CyverseClient(**result)
            #print("?{}".format(type(self._client)))
            #print("??{}: {}".format(type(self._client), self._client))
        except KeyboardInterrupt:
            raise

    # check if provided client app is in user's client app list
    def check_clients(self):
        for client in self._api.clients.list():
            if client.name == self._client.clientName:
                print("{} == {}".format(client.name, self._client.clientName))
                return True
        else:
            return False

    def delete_client(self, check_remote=False):
        client = self._client
        if check_remote:
            if not self.check_clients():
                print("Client {} is not in client list".format(client.name))
                return
        # if client name is string
        # if type(client) is str:
        #    client_name = client
        # else:
        client_name = client.clientName
        self._api.clients.delete(clientName=client_name)

    def _verify_client(self):
        # we need to overwrite the existing client to get the consumerKey
        # consumerSecret if they have not been provided
        has_consumer_info = self._client.consumerKey and self._client.consumerSecret
        return self.create_client(not has_consumer_info)

# a 'bucket' is a top-level directory you have access to.
#    default is the user's name (home directory)
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

    @property
    def delete(self):
        pass


class CyverseBucketContainer(BaseBucketContainer):
    def __init__(self, provider, bucket):
        self._file_handle = provider._conn._api.files
        super(CyverseBucketContainer, self).__init__(provider, bucket)
        self.details = {"filePath": provider._get_config_value("file_path"),
                        "systemId": self._provider.CYVERSE_SYSTEM_ID}

    def _get_files(self):
        pass

    def get(self, name):
        pass
#        try:
#            # pylint:disable=protected-access
#            obj = self.bucket._bucket.Object(name)

    def list(self, limit=None, marker=None):
        #print(self.details)
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
        self.file_reqs = {
            "filePath": "/{}/".format(provider._get_config_value("file_path")),
            "systemId": provider.CYVERSE_SYSTEM_ID
        }
        self._info = obj

    # helper functions that interact directly with cyverse
    def _get_file(self, path):
        info = None
        with open(path, "rb") as f:
            info = self._provider._conn.files.importData(fileToUpload=f,
                                                         **self.FILE_CONF)
        return info

    def _mkdir(self, path):
        body = {
            "action" : "mkdir",
            "path" : path
        }
        file_reqs = {
            "systemId" : "",
            "filePath" : ""
        }
        return self._provider._conn.files.manage(body=body, **file_reqs)

    def _list_local_files(self, path):
        def _is_subpath(a, bs):
            for b in bs:
                if len(a) < len(b) and a in b:
                    return True
            return False

        fpath = abspath(path)
        if isdir(fpath):
            fullpath_files = []
            fullpath_dirs = []
            for dirpath, dirnames, filenames in walk(fpath):
                fullpath_files.extend(map(
                    lambda x: dirpath + "/" + x,
                    filenames
                ))
                fullpath_dirs.extend(map(
                    lambda x: dirpath + "/" + x,
                    dirnames
                ))
            fullpath_dirs = list(filter(lambda x: not _is_subpath(x, fullpath_dirs), fullpath_dirs))
            return (fullpath_dirs, fullpath_files)
        return ([], [fpath])

    def _upload(self, path):
        folders, files = _list_local_files(path)
        for folder in folders:
            _mkdir(folder)
        for file in files:
            _upload_file


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
