from dl_connector_bundle_chs3.chs3_gsheets.core.data_source import GSheetsFileS3DataSource
from dl_connector_bundle_chs3.chs3_gsheets.core.data_source_spec import GSheetsFileS3DataSourceSpec
from dl_connector_bundle_chs3.chs3_gsheets.core.us_connection import GSheetsFileS3Connection
from dl_connector_bundle_chs3_tests.db.base.core.data_source import CHS3TableDataSourceTestBase
from dl_connector_bundle_chs3_tests.db.gsheets_v2.core.base import BaseGSheetsFileS3TestClass


class TestGSheetsFileS3TableDataSource(
    BaseGSheetsFileS3TestClass,
    CHS3TableDataSourceTestBase[GSheetsFileS3Connection, GSheetsFileS3DataSourceSpec, GSheetsFileS3DataSource],
):
    DSRC_CLS = GSheetsFileS3DataSource
