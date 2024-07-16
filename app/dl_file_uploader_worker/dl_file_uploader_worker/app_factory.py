import attr

from dl_api_commons.tenant_resolver import (
    CommonTenantResolver,
    TenantResolver,
)
from dl_file_uploader_worker.app_settings import FileUploaderWorkerSettingsOS
from dl_file_uploader_worker_lib.app import FileUploaderWorkerFactory


@attr.s(kw_only=True)
class StandaloneFileUploaderWorkerFactory(FileUploaderWorkerFactory[FileUploaderWorkerSettingsOS]):
    def _get_tenant_resolver(self) -> TenantResolver:
        return CommonTenantResolver()
