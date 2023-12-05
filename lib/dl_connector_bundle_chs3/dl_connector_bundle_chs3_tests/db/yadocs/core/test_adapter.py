from dl_core_testing.testcases.adapter import BaseAsyncAdapterTestClass

from dl_connector_bundle_chs3.chs3_base.core.target_dto import BaseFileS3ConnTargetDTO
from dl_connector_bundle_chs3.chs3_yadocs.core.adapter import AsyncYaDocsFileS3Adapter
from dl_connector_bundle_chs3_tests.db.yadocs.core.base import BaseYaDocsFileS3TestClass


class TestAsyncYaDocsFileS3Adapter(
    BaseYaDocsFileS3TestClass,
    BaseAsyncAdapterTestClass[BaseFileS3ConnTargetDTO],
):
    ASYNC_ADAPTER_CLS = AsyncYaDocsFileS3Adapter
