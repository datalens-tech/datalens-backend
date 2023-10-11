from dl_core_testing.testcases.adapter import BaseAsyncAdapterTestClass

from dl_connector_bundle_chs3.chs3_base.core.target_dto import BaseFileS3ConnTargetDTO
from dl_connector_bundle_chs3.file.core.adapter import AsyncFileS3Adapter
from dl_connector_bundle_chs3_tests.db.file.core.base import BaseFileS3TestClass


class TestAsyncFileS3Adapter(
    BaseFileS3TestClass,
    BaseAsyncAdapterTestClass[BaseFileS3ConnTargetDTO],
):
    ASYNC_ADAPTER_CLS = AsyncFileS3Adapter
    test_debug_query_with_default_value = True
