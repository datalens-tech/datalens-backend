from typing import Optional

import arq
import attr

from dl_api_commons.base_models import RequestContextInfo
from dl_api_commons.retrier.policy import (
    BaseRetryPolicyFactory,
    RetryPolicyFactory,
)
from dl_api_commons.tenant_resolver import TenantResolver
from dl_configs.crypto_keys import CryptoKeysConfig
from dl_core.aio.web_app_services.gsheets import GSheetsSettings
from dl_core.aio.web_app_services.redis import RedisBaseService
from dl_core.services_registry.top_level import ServicesRegistry
from dl_core.us_manager.us_manager_async import AsyncUSManager
from dl_file_uploader_task_interface.utils_service_registry import (
    create_sr_factory_from_env_vars,
    get_async_service_us_manager,
)
from dl_file_uploader_worker_lib.settings import FileUploaderWorkerSettings
from dl_s3.s3_service import S3Service
from dl_task_processor.context import BaseContext
from dl_task_processor.processor import (
    TaskProcessor,
    make_task_processor,
)
from dl_utils.aio import ContextVarExecutor


@attr.s
class SecureReaderSettings:
    socket: str = attr.ib()
    endpoint: Optional[str] = attr.ib(default=None)
    cafile: Optional[str] = attr.ib(default=None)


@attr.s
class FileUploaderTaskContext(BaseContext):
    settings: FileUploaderWorkerSettings = attr.ib()
    tpe: ContextVarExecutor = attr.ib()
    redis_service: RedisBaseService = attr.ib()
    s3_service: S3Service = attr.ib()
    gsheets_settings: GSheetsSettings = attr.ib()
    redis_pool: arq.ArqRedis = attr.ib()
    crypto_keys_config: CryptoKeysConfig = attr.ib()
    secure_reader_settings: SecureReaderSettings = attr.ib()
    tenant_resolver: TenantResolver = attr.ib()
    ca_data: bytes = attr.ib()

    def get_rci(self) -> RequestContextInfo:
        return RequestContextInfo.create_empty()

    def get_service_registry(self, rci: Optional[RequestContextInfo] = None) -> ServicesRegistry:
        rci = rci or RequestContextInfo.create_empty()
        return create_sr_factory_from_env_vars(
            self.settings.CONNECTORS,
            ca_data=self.ca_data,
        ).make_service_registry(rci)

    def get_retry_policy_factory(self) -> BaseRetryPolicyFactory:
        return RetryPolicyFactory(self.settings.US_CLIENT.RETRY_POLICY)

    def get_async_usm(self, rci: Optional[RequestContextInfo] = None) -> AsyncUSManager:
        rci = rci or RequestContextInfo.create_empty()
        services_registry = self.get_service_registry(rci=rci)
        retry_policy_factory = self.get_retry_policy_factory()
        return get_async_service_us_manager(
            us_host=self.settings.US_BASE_URL,
            us_master_token=self.settings.US_MASTER_TOKEN,
            services_registry=services_registry,
            bi_context=rci,
            crypto_keys_config=self.crypto_keys_config,
            ca_data=self.ca_data,
            retry_policy_factory=retry_policy_factory,
        )

    def make_task_processor(self, request_id: Optional[str]) -> TaskProcessor:
        return make_task_processor(
            redis_pool=self.redis_pool,
            request_id=request_id,
        )
