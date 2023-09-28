from dl_api_lib_testing.connector.data_api_suites import (
    DefaultConnectorDataDistinctTestSuite,
    DefaultConnectorDataGroupByFormulaTestSuite,
    DefaultConnectorDataPreviewTestSuite,
    DefaultConnectorDataRangeTestSuite,
)
from dl_connector_bundle_chs3_tests.db.base.api.data import CHS3DataResultTestSuite
from dl_connector_bundle_chs3_tests.db.file.api.base import FileS3DataApiTestBase
from dl_testing.regulated_test import RegulatedTestParams


class TestFileS3DataResult(FileS3DataApiTestBase, CHS3DataResultTestSuite):
    test_params = RegulatedTestParams(
        mark_features_skipped={
            CHS3DataResultTestSuite.array_support: "File connector doesn't support arrays",
        }
    )


class TestFileS3DataGroupBy(FileS3DataApiTestBase, DefaultConnectorDataGroupByFormulaTestSuite):
    pass


class TestFileS3DataRange(FileS3DataApiTestBase, DefaultConnectorDataRangeTestSuite):
    pass


class TestFileDataDistinct(FileS3DataApiTestBase, DefaultConnectorDataDistinctTestSuite):
    test_params = RegulatedTestParams(
        mark_tests_failed={
            DefaultConnectorDataDistinctTestSuite.test_date_filter_distinct: "FIXME",
        }
    )


class TestFileS3DataPreview(FileS3DataApiTestBase, DefaultConnectorDataPreviewTestSuite):
    pass
