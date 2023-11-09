from __future__ import annotations

from dl_connector_bundle_chs3.file.core.us_connection import FileS3Connection
from dl_connector_bundle_chs3_tests.db.base.core.connection import CHS3ConnectionTestBase
from dl_connector_bundle_chs3_tests.db.yadocs.core.base import BaseYaDocsFileS3TestClass


class TestYaDocsFileS3Connection(BaseYaDocsFileS3TestClass, CHS3ConnectionTestBase[FileS3Connection]):
    pass
