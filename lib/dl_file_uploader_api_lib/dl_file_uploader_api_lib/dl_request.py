import dl_auth
from dl_core.aio.aiohttp_wrappers_data_core import DLRequestDataCore
from dl_file_uploader_api_lib.aiohttp_services.crypto import CryptoService
from dl_file_uploader_api_lib.aiohttp_services.s3_service import InternalS3Service
from dl_file_uploader_api_lib.aiohttp_services.us_auth_provider_factory import USAuthProviderFactoryService
from dl_file_uploader_api_lib.aiohttp_services.us_client import USEntriesClientService
from dl_file_uploader_lib import exc
from dl_file_uploader_lib.redis_model.base import RedisModelManager
from dl_file_uploader_lib.s3_model.base import S3ModelManager
from dl_s3.s3_service import S3Service
from dl_task_processor.arq_redis import ArqRedisService
from dl_task_processor.processor import (
    TaskProcessor,
    make_task_processor,
)
import dl_us_entries_client


class FileUploaderDLRequest(DLRequestDataCore):
    def get_us_entries_client(self) -> dl_us_entries_client.USEntriesAsyncClient:
        return USEntriesClientService.get_app_instance(self.request.app).client

    def get_us_auth_provider(self) -> dl_auth.AuthProviderProtocol:
        auth_data = self.rci.auth_data
        if auth_data is None:
            raise exc.PermissionDenied("No auth data available on request context")
        factory = USAuthProviderFactoryService.get_app_instance(self.request.app).factory
        return factory.create(auth_data)

    def get_s3_service_for_uploads(self) -> S3Service:
        return S3Service.get_app_instance(self.request.app)

    def get_internal_s3_service(self) -> InternalS3Service:
        return InternalS3Service.get_app_instance(self.request.app)

    def get_task_processor(self) -> TaskProcessor:
        return make_task_processor(
            redis_pool=ArqRedisService.get_app_instance(self.request.app).get_arq_pool(),
            request_id=self.rci.request_id,
        )

    def get_redis_model_manager(self) -> RedisModelManager:
        return RedisModelManager(
            redis=self.get_persistent_redis(),
            rci=self.rci,
            crypto_keys_config=CryptoService.get_config_for_app(self.request.app),
        )

    def get_s3_model_manager(self) -> S3ModelManager:
        assert self.rci.tenant is not None
        tenant_id = self.rci.tenant.get_tenant_id()

        return S3ModelManager(
            s3_service=self.get_internal_s3_service(),
            tenant_id=tenant_id,
            rci=self.rci,
            crypto_keys_config=CryptoService.get_config_for_app(self.request.app),
        )
