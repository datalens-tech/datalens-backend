from __future__ import annotations

from dl_connector_bundle_chs3.file.core.us_connection import FileS3Connection
from dl_connector_bundle_chs3_tests.db.base.core.connection import CHS3ConnectionTestBase
from dl_connector_bundle_chs3_tests.db.gsheets_v2.core.base import BaseGSheetsFileS3TestClass


class TestGSheetsFileS3Connection(BaseGSheetsFileS3TestClass, CHS3ConnectionTestBase[FileS3Connection]):
    pass
