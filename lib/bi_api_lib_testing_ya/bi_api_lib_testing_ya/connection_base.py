import abc

from dl_api_lib_testing.connection_base import ConnectionTestBase
from bi_api_lib_testing_ya.base import BiApiTestPrivateBase


class ConnectionTestPrivateBase(BiApiTestPrivateBase, ConnectionTestBase, abc.ABC):
    pass
