import attr

from bi_connector_bundle_chs3.chs3_base.core.connection_executors import BaseFileS3AsyncAdapterConnExecutor
from bi_connector_bundle_chs3.file.core.adapter import AsyncFileS3Adapter


@attr.s(cmp=False, hash=False)
class FileS3AsyncAdapterConnExecutor(BaseFileS3AsyncAdapterConnExecutor):
    TARGET_ADAPTER_CLS = AsyncFileS3Adapter  # type: ignore  # TODO: FIX
