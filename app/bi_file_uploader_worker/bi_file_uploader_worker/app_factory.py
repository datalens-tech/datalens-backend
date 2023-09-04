import attr

from bi_configs.enums import AppType

from bi_file_uploader_worker_lib.app import FileUploaderWorkerFactory

from bi_api_commons.tenant_resolver import TenantResolver, CommonTenantResolver
from bi_api_commons_ya_cloud.tenant_resolver import TenantResolverYC, TenantResolverDC

from bi_file_uploader_worker.app_settings import DefaultFileUploaderWorkerSettings


@attr.s(kw_only=True)
class DefaultFileUploaderWorkerFactory(FileUploaderWorkerFactory[DefaultFileUploaderWorkerSettings]):
    def _get_tenant_resolver(self) -> TenantResolver:
        return {
            AppType.CLOUD: TenantResolverYC(),
            AppType.DATA_CLOUD: TenantResolverDC(),
            AppType.NEBIUS: TenantResolverYC(),
        }.get(self._settings.APP_TYPE, CommonTenantResolver())
