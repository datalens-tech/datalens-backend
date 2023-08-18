from typing import Optional, Any

import attr
import arq

from bi_configs.crypto_keys import CryptoKeysConfig
from bi_utils.aio import ContextVarExecutor

from bi_api_commons.base_models import RequestContextInfo
from bi_core.aio.web_app_services.gsheets import GSheetsSettings
from bi_core.aio.web_app_services.s3 import S3Service
from bi_core.aio.web_app_services.redis import RedisBaseService
from bi_core.us_manager.us_manager_async import AsyncUSManager
from bi_core.services_registry.top_level import ServicesRegistry

from bi_task_processor.context import BaseContext
from bi_task_processor.processor import TaskProcessor, make_task_processor
from bi_file_uploader_task_interface.utils_service_registry import (
    create_sr_factory_from_env_vars,
    get_async_service_us_manager,
)


@attr.s
class FileUploaderTaskContext(BaseContext):
    settings: Optional[Any] = attr.ib()
    tpe: ContextVarExecutor = attr.ib()
    redis_service: RedisBaseService = attr.ib()
    s3_service: S3Service = attr.ib()
    gsheets_settings: GSheetsSettings = attr.ib()
    redis_pool: arq.ArqRedis = attr.ib()
    crypto_keys_config: CryptoKeysConfig = attr.ib()
    secure_reader_socket: str = attr.ib()

    def get_rci(self) -> RequestContextInfo:
        return RequestContextInfo.create_empty()

    def get_service_registry(self, rci: Optional[RequestContextInfo] = None) -> ServicesRegistry:
        rci = rci or RequestContextInfo.create_empty()
        return create_sr_factory_from_env_vars(
            self.settings.CONNECTORS,
        ).make_service_registry(rci)

    def get_async_usm(self, rci: Optional[RequestContextInfo] = None) -> AsyncUSManager:
        rci = rci or RequestContextInfo.create_empty()
        services_registry = self.get_service_registry(rci=rci)
        return get_async_service_us_manager(
            us_host=self.settings.US_BASE_URL,
            us_master_token=self.settings.US_MASTER_TOKEN,
            services_registry=services_registry,
            bi_context=rci,
            crypto_keys_config=self.crypto_keys_config,
        )

    def make_task_processor(self, request_id: Optional[str]) -> TaskProcessor:
        return make_task_processor(
            redis_pool=self.redis_pool,
            request_id=request_id,
        )
