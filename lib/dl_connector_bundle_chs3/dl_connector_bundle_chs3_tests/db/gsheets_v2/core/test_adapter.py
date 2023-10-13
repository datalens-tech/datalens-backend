from dl_core_testing.testcases.adapter import BaseAsyncAdapterTestClass

from dl_connector_bundle_chs3.chs3_base.core.target_dto import BaseFileS3ConnTargetDTO
from dl_connector_bundle_chs3.chs3_gsheets.core.adapter import AsyncGSheetsFileS3Adapter
from dl_connector_bundle_chs3_tests.db.gsheets_v2.core.base import BaseGSheetsFileS3TestClass


class TestAsyncGSheetsFileS3Adapter(
    BaseGSheetsFileS3TestClass,
    BaseAsyncAdapterTestClass[BaseFileS3ConnTargetDTO],
):
    ASYNC_ADAPTER_CLS = AsyncGSheetsFileS3Adapter
