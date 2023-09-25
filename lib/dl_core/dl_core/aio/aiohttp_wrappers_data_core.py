from typing import TYPE_CHECKING

import redis.asyncio

from dl_api_commons.aiohttp.aiohttp_wrappers import (
    DLRequestBase,
    DLRequestView,
)
from dl_constants.enums import (
    ProcessorType,
    RedisInstanceKind,
)
from dl_core.aio.web_app_services.data_processing.data_processor import DataProcessorService
from dl_core.aio.web_app_services.data_processing.factory import get_data_processor_service_class
from dl_core.aio.web_app_services.redis import RedisBaseService
from dl_core.us_manager.us_manager_async import AsyncUSManager


if TYPE_CHECKING:
    from dl_core.services_registry import ServicesRegistry  # noqa


class DLRequestDataCore(DLRequestBase):
    KEY_US_MANAGER = "us_manager"
    KEY_SERVICE_US_MANAGER = "service_us_manager"
    KEY_SERVICES_REGISTRY = "services_registry"

    @property
    def us_manager(self) -> AsyncUSManager:
        """US manager associated with request's user"""
        return self.request.get(self.KEY_US_MANAGER)  # type: ignore  # TODO: fix

    @us_manager.setter
    def us_manager(self, value: AsyncUSManager) -> None:
        self._set_attr_once(self.KEY_US_MANAGER, value)

    @property
    def service_us_manager(self) -> AsyncUSManager:
        """US manager with master token"""
        return self.request.get(self.KEY_SERVICE_US_MANAGER)  # type: ignore  # TODO: fix

    @service_us_manager.setter
    def service_us_manager(self, value: AsyncUSManager) -> None:
        self._set_attr_once(self.KEY_SERVICE_US_MANAGER, value)

    @property
    def services_registry(self) -> "ServicesRegistry":
        return self.request.get(self.KEY_SERVICES_REGISTRY)  # type: ignore  # TODO: fix

    @services_registry.setter
    def services_registry(self, value: "ServicesRegistry") -> None:
        self._set_attr_once(self.KEY_SERVICES_REGISTRY, value)

    def get_caches_redis(self, allow_slave: bool = False) -> redis.asyncio.Redis:
        return RedisBaseService.get_app_instance(
            self.request.app,
            RedisInstanceKind.caches,
        ).get_redis(allow_slave=allow_slave)

    def get_persistent_redis(self, allow_slave: bool = False) -> redis.asyncio.Redis:
        return RedisBaseService.get_app_instance(
            self.request.app,
            RedisInstanceKind.persistent,
        ).get_redis(allow_slave=allow_slave)

    def get_mutations_redis(self, allow_slave: bool = False) -> redis.asyncio.Redis:
        return RedisBaseService.get_app_instance(
            self.request.app,
            RedisInstanceKind.mutations,
        ).get_redis(allow_slave=allow_slave)

    def get_data_processor_service(self, processor_type: ProcessorType) -> DataProcessorService:
        return get_data_processor_service_class(processor_type).get_app_instance(self.request.app)


class DLRequestDataCoreView(DLRequestView[DLRequestDataCore]):
    dl_request_cls = DLRequestDataCore
