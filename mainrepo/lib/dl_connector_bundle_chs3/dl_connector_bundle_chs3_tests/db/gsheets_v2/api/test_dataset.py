from dl_connector_bundle_chs3_tests.db.base.api.dataset import CHS3DatasetTestSuite
from dl_connector_bundle_chs3_tests.db.gsheets_v2.api.base import GSheetsFileS3DatasetTestBase


class TestGSheetsFileS3Dataset(GSheetsFileS3DatasetTestBase, CHS3DatasetTestSuite):
    pass
