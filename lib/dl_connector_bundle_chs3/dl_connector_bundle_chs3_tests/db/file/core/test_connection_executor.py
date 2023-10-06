from dl_connector_bundle_chs3.file.core.us_connection import FileS3Connection
from dl_connector_bundle_chs3_tests.db.base.core.connection_executor import (
    CHS3AsyncConnectionExecutorTestBase,
    CHS3SyncConnectionExecutorTestBase,
)
from dl_connector_bundle_chs3_tests.db.file.core.base import BaseFileS3TestClass


class TestFileS3SyncConnectionExecutor(BaseFileS3TestClass, CHS3SyncConnectionExecutorTestBase[FileS3Connection]):
    pass


class TestFileS3AsyncConnectionExecutor(BaseFileS3TestClass, CHS3AsyncConnectionExecutorTestBase[FileS3Connection]):
    pass
