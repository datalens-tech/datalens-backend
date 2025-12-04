import abc
from typing import (
    Generic,
    TypeVar,
)

import attr
import temporalio.worker
from typing_extensions import override

import dl_app_base
import dl_settings
import dl_temporal.base as base
import dl_temporal.client as client
import dl_temporal.worker as worker


class TemporalWorkerSettings(dl_settings.BaseSettings):
    TASK_QUEUE: str


class TemporalWorkerAppSettingsMixin(dl_app_base.BaseAppSettings):
    TEMPORAL_CLIENT: client.TemporalClientSettings = NotImplemented
    TEMPORAL_WORKER: TemporalWorkerSettings = NotImplemented


@attr.define(frozen=True, kw_only=True)
class TemporalWorkerAppMixin(dl_app_base.BaseApp):
    ...


AppType = TypeVar("AppType", bound=TemporalWorkerAppMixin)


@attr.define(kw_only=True, slots=False)
class TemporalWorkerAppFactoryMixin(
    dl_app_base.BaseAppFactory[AppType],
    Generic[AppType],
):
    settings: TemporalWorkerAppSettingsMixin

    @override
    @dl_app_base.singleton_class_method_result
    async def _get_main_callbacks(
        self,
    ) -> list[dl_app_base.Callback]:
        result = await super()._get_main_callbacks()

        temporal_worker = await self._get_temporal_worker()
        result.append(dl_app_base.Callback(name="temporal_worker", coroutine=temporal_worker.run()))

        return result

    @override
    @dl_app_base.singleton_class_method_result
    async def _get_shutdown_callbacks(
        self,
    ) -> list[dl_app_base.Callback]:
        result = await super()._get_shutdown_callbacks()

        temporal_client = await self._get_temporal_client()
        result.append(dl_app_base.Callback(name="temporal_client", coroutine=temporal_client.close()))

        return result

    @dl_app_base.singleton_class_method_result
    async def _get_temporal_client(
        self,
    ) -> client.TemporalClient:
        return await client.TemporalClient.from_dependencies(
            dependencies=client.TemporalClientDependencies(
                namespace=self.settings.TEMPORAL_CLIENT.NAMESPACE,
                host=self.settings.TEMPORAL_CLIENT.HOST,
                port=self.settings.TEMPORAL_CLIENT.PORT,
                tls=self.settings.TEMPORAL_CLIENT.TLS,
                lazy=False,  # Can't create worker with lazy client
                metadata_provider=await self._get_temporal_client_metadata_provider(),
            ),
        )

    @dl_app_base.singleton_class_method_result
    async def _get_temporal_worker(
        self,
    ) -> temporalio.worker.Worker:
        return worker.create_worker(
            task_queue=self.settings.TEMPORAL_WORKER.TASK_QUEUE,
            client=await self._get_temporal_client(),
            workflows=await self._get_temporal_workflows(),
            activities=await self._get_temporal_activities(),
        )

    @abc.abstractmethod
    @dl_app_base.singleton_class_method_result
    async def _get_temporal_workflows(
        self,
    ) -> list[type[base.WorkflowProtocol]]:
        ...

    @abc.abstractmethod
    @dl_app_base.singleton_class_method_result
    async def _get_temporal_activities(
        self,
    ) -> list[base.ActivityProtocol]:
        ...

    @abc.abstractmethod
    @dl_app_base.singleton_class_method_result
    async def _get_temporal_client_metadata_provider(
        self,
    ) -> client.MetadataProvider:
        ...
