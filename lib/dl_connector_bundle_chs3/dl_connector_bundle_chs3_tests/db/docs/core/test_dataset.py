from dl_connector_bundle_chs3.chs3_yadocs.core.us_connection import YaDocsFileS3Connection
from dl_connector_bundle_chs3_tests.db.base.core.dataset import CHS3DatasetTestBase
from dl_connector_bundle_chs3_tests.db.docs.core.base import BaseYaDocsFileS3TestClass


class TestYaDocsFileS3Dataset(BaseYaDocsFileS3TestClass, CHS3DatasetTestBase[YaDocsFileS3Connection]):
    pass
