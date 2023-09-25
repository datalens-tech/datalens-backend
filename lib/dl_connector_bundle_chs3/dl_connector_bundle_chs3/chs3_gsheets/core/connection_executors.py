import attr

from dl_connector_bundle_chs3.chs3_base.core.connection_executors import BaseFileS3AsyncAdapterConnExecutor
from dl_connector_bundle_chs3.chs3_gsheets.core.adapter import AsyncGSheetsFileS3Adapter


@attr.s(cmp=False, hash=False)
class GSheetsFileS3AsyncAdapterConnExecutor(BaseFileS3AsyncAdapterConnExecutor):
    TARGET_ADAPTER_CLS = AsyncGSheetsFileS3Adapter  # type: ignore  # TODO: FIX
