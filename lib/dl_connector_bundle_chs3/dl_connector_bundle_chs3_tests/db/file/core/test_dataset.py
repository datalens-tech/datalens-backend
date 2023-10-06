from dl_connector_bundle_chs3.file.core.us_connection import FileS3Connection
from dl_connector_bundle_chs3_tests.db.base.core.dataset import CHS3DatasetTestBase
from dl_connector_bundle_chs3_tests.db.file.core.base import BaseFileS3TestClass


class TestFileS3Dataset(BaseFileS3TestClass, CHS3DatasetTestBase[FileS3Connection]):
    pass
