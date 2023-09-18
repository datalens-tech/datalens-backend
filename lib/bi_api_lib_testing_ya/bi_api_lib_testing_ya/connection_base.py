import abc

from bi_api_lib_testing_ya.base import BiApiTestPrivateBase
from dl_api_lib_testing.connection_base import ConnectionTestBase


class ConnectionTestPrivateBase(BiApiTestPrivateBase, ConnectionTestBase, abc.ABC):
    pass
