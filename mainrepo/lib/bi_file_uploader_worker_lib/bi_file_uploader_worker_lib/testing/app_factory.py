import attr

from bi_api_commons.tenant_resolver import TenantResolver, CommonTenantResolver

from bi_file_uploader_worker_lib.app import FileUploaderWorkerFactory
from bi_file_uploader_worker_lib.settings import FileUploaderWorkerSettings


@attr.s(kw_only=True)
class TestingFileUploaderWorkerFactory(FileUploaderWorkerFactory[FileUploaderWorkerSettings]):
    def _get_tenant_resolver(self) -> TenantResolver:
        return CommonTenantResolver()
