import attr

from bi_api_commons_ya_cloud.tenant_resolver import TenantResolverYC
from dl_api_commons.tenant_resolver import TenantResolver
from dl_file_uploader_worker_lib.app import FileUploaderWorkerFactory
from dl_file_uploader_worker_lib.settings import FileUploaderWorkerSettings


@attr.s(kw_only=True)
class FileUploaderWorkerFactoryYC(FileUploaderWorkerFactory[FileUploaderWorkerSettings]):
    def _get_tenant_resolver(self) -> TenantResolver:
        return TenantResolverYC()
