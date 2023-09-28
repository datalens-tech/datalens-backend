from dl_connector_bundle_chs3.file.core.data_source import FileS3DataSource
from dl_connector_bundle_chs3.file.core.data_source_spec import FileS3DataSourceSpec
from dl_connector_bundle_chs3.file.core.us_connection import FileS3Connection
from dl_connector_bundle_chs3_tests.db.base.core.data_source import CHS3TableDataSourceTestBase
from dl_connector_bundle_chs3_tests.db.file.core.base import BaseFileS3TestClass


class TestFileS3TableDataSource(
    BaseFileS3TestClass,
    CHS3TableDataSourceTestBase[FileS3Connection, FileS3DataSourceSpec, FileS3DataSource],
):
    DSRC_CLS = FileS3DataSource
