import attr

from bi_api_commons_ya_cloud.tenant_resolver import (
    TenantResolverDC,
    TenantResolverYC,
)
from bi_file_uploader_worker.app_settings import DefaultFileUploaderWorkerSettings
from dl_api_commons.tenant_resolver import (
    CommonTenantResolver,
    TenantResolver,
)
from dl_configs.enums import AppType
from dl_file_uploader_worker_lib.app import FileUploaderWorkerFactory


@attr.s(kw_only=True)
class DefaultFileUploaderWorkerFactory(FileUploaderWorkerFactory[DefaultFileUploaderWorkerSettings]):
    def _get_tenant_resolver(self) -> TenantResolver:
        return {
            AppType.CLOUD: TenantResolverYC(),
            AppType.DATA_CLOUD: TenantResolverDC(),
            AppType.NEBIUS: TenantResolverYC(),
        }.get(self._settings.APP_TYPE, CommonTenantResolver())
