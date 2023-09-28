from dl_connector_bundle_chs3.file.core.us_connection import FileS3Connection
from dl_connector_bundle_chs3_tests.db.base.core.connection_executor import (
    CHS3AsyncConnectionExecutorTestBase,
    CHS3SyncConnectionExecutorTestBase,
)
from dl_connector_bundle_chs3_tests.db.gsheets_v2.core.base import BaseGSheetsFileS3TestClass


class TestGSheetsFileS3SyncConnectionExecutor(
    BaseGSheetsFileS3TestClass,
    CHS3SyncConnectionExecutorTestBase[FileS3Connection],
):
    pass


class TestGSheetsFileS3AsyncConnectionExecutor(
    BaseGSheetsFileS3TestClass,
    CHS3AsyncConnectionExecutorTestBase[FileS3Connection],
):
    pass
