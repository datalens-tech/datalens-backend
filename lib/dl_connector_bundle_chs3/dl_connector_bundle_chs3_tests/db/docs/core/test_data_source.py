from dl_connector_bundle_chs3.chs3_yadocs.core.data_source import YaDocsFileS3DataSource
from dl_connector_bundle_chs3.chs3_yadocs.core.data_source_spec import YaDocsFileS3DataSourceSpec
from dl_connector_bundle_chs3.chs3_yadocs.core.us_connection import YaDocsFileS3Connection
from dl_connector_bundle_chs3_tests.db.base.core.data_source import CHS3TableDataSourceTestBase
from dl_connector_bundle_chs3_tests.db.docs.core.base import BaseYaDocsFileS3TestClass


class TestYaDocsFileS3TableDataSource(
    BaseYaDocsFileS3TestClass,
    CHS3TableDataSourceTestBase[YaDocsFileS3Connection, YaDocsFileS3DataSourceSpec, YaDocsFileS3DataSource],
):
    DSRC_CLS = YaDocsFileS3DataSource
