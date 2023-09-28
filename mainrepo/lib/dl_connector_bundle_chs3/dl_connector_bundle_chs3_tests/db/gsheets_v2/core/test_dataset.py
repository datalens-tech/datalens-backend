from dl_connector_bundle_chs3.chs3_gsheets.core.us_connection import GSheetsFileS3Connection
from dl_connector_bundle_chs3_tests.db.base.core.dataset import CHS3DatasetTestBase
from dl_connector_bundle_chs3_tests.db.gsheets_v2.core.base import BaseGSheetsFileS3TestClass


class TestGSheetsFileS3Dataset(BaseGSheetsFileS3TestClass, CHS3DatasetTestBase[GSheetsFileS3Connection]):
    pass
