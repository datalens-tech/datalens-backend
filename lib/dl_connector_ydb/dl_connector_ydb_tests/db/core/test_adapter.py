from dl_core_testing.testcases.adapter import BaseAsyncAdapterTestClass
from dl_testing.regulated_test import RegulatedTestParams

from dl_connector_ydb.core.ydb.adapter import YDBAdapter
from dl_connector_ydb.core.ydb.target_dto import YDBConnTargetDTO
from dl_connector_ydb_tests.db.core.base import BaseYDBTestClass


class TestAsyncYDBAdapter(
    BaseYDBTestClass,
    BaseAsyncAdapterTestClass[YDBConnTargetDTO],
):
    test_params = RegulatedTestParams(
        mark_tests_failed={
            BaseAsyncAdapterTestClass.test_pass_db_query_to_user: "BI-5885: Error transformer not implemented",  # TODO: FIXME
            BaseAsyncAdapterTestClass.test_default_pass_db_query_to_user: "BI-5885: Error transformer not implemented",  # TODO: FIXME
            BaseAsyncAdapterTestClass.test_timeout: "BI-5885: Error transformer not implemented",  # TODO: FIXME
        },
    )

    ASYNC_ADAPTER_CLS = YDBAdapter
