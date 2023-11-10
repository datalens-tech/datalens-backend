import attr

from dl_connector_bundle_chs3.chs3_base.core.connection_executors import BaseFileS3AsyncAdapterConnExecutor
from dl_connector_bundle_chs3.chs3_yadocs.core.adapter import AsyncYaDocsFileS3Adapter


@attr.s(cmp=False, hash=False)
class YaDocsFileS3AsyncAdapterConnExecutor(BaseFileS3AsyncAdapterConnExecutor):
    TARGET_ADAPTER_CLS = AsyncYaDocsFileS3Adapter  # type: ignore  # TODO: FIX
