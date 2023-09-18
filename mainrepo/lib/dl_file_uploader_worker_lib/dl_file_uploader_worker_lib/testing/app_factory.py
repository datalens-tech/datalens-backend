import attr

from dl_api_commons.tenant_resolver import (
    CommonTenantResolver,
    TenantResolver,
)
from dl_file_uploader_worker_lib.app import FileUploaderWorkerFactory
from dl_file_uploader_worker_lib.settings import FileUploaderWorkerSettings


@attr.s(kw_only=True)
class TestingFileUploaderWorkerFactory(FileUploaderWorkerFactory[FileUploaderWorkerSettings]):
    def _get_tenant_resolver(self) -> TenantResolver:
        return CommonTenantResolver()
