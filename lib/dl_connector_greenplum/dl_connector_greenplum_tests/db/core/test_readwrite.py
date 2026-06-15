from dl_core_testing.testcases.connection_executor import ReadWriteAdapterTestSuite

from dl_connector_greenplum.core.us_connection import GreenplumConnection
from dl_connector_greenplum_tests.db.core.base import BaseGreenplumTestClass


class TestGreenplumReadWrite(
    BaseGreenplumTestClass,
    ReadWriteAdapterTestSuite[GreenplumConnection],
):
    pass
