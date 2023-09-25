import abc

from bi_api_lib_testing_ya.base import ApiTestPrivateBase
from dl_api_lib_testing.connection_base import ConnectionTestBase


class ConnectionTestPrivateBase(ApiTestPrivateBase, ConnectionTestBase, abc.ABC):
    pass
