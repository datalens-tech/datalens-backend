from dl_connector_bundle_chs3.file.core.us_connection import FileS3Connection
from dl_connector_bundle_chs3_tests.db.base.core.connection_executor import (
    CHS3AsyncConnectionExecutorTestBase,
    CHS3SyncConnectionExecutorTestBase,
)
from dl_connector_bundle_chs3_tests.db.docs.core.base import BaseYaDocsFileS3TestClass


class TestYaDocsFileS3SyncConnectionExecutor(
    BaseYaDocsFileS3TestClass,
    CHS3SyncConnectionExecutorTestBase[FileS3Connection],
):
    pass


class TestYaDocsFileS3AsyncConnectionExecutor(
    BaseYaDocsFileS3TestClass,
    CHS3AsyncConnectionExecutorTestBase[FileS3Connection],
):
    pass
