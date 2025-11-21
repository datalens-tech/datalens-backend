import abc
from typing import (
    Generic,
    TypeVar,
)

import attr
import temporalio.worker
from typing_extensions import override

import dl_settings
import dl_temporal.base as base
import dl_temporal.client as client
import dl_temporal.utils.app as app_utils
import dl_temporal.utils.singleton as singleton_utils
import dl_temporal.worker as worker


class TemporalWorkerSettings(dl_settings.BaseSettings):
    task_queue: str


class TemporalWorkerAppSettingsMixin(app_utils.BaseAppSettings):
    temporal_client: client.TemporalClientSettings = NotImplemented
    temporal_worker: TemporalWorkerSettings = NotImplemented


@attr.define(frozen=True, kw_only=True)
class TemporalWorkerAppMixin(app_utils.BaseApp):
    ...


AppType = TypeVar("AppType", bound=TemporalWorkerAppMixin)


@attr.define(kw_only=True, slots=False)
class TemporalWorkerAppFactoryMixin(
    app_utils.BaseAppFactory[AppType],
    Generic[AppType],
):
    settings: TemporalWorkerAppSettingsMixin

    @override
    @singleton_utils.singleton_class_method_result
    async def _get_main_callbacks(
        self,
    ) -> list[app_utils.Callback]:
        result = await super()._get_main_callbacks()

        temporal_worker = await self._get_temporal_worker()
        result.append(app_utils.Callback(name="temporal_worker", coroutine=temporal_worker.run()))

        return result

    @override
    @singleton_utils.singleton_class_method_result
    async def _get_shutdown_callbacks(
        self,
    ) -> list[app_utils.Callback]:
        result = await super()._get_shutdown_callbacks()

        temporal_client = await self._get_temporal_client()
        result.append(app_utils.Callback(name="temporal_client", coroutine=temporal_client.close()))

        return result

    @singleton_utils.singleton_class_method_result
    async def _get_temporal_client(
        self,
    ) -> client.TemporalClient:
        return await client.TemporalClient.from_dependencies(
            dependencies=client.TemporalClientDependencies(
                namespace=self.settings.temporal_client.namespace,
                host=self.settings.temporal_client.host,
                port=self.settings.temporal_client.port,
                tls=self.settings.temporal_client.tls,
                lazy=False,
                metadata_provider=await self._get_temporal_client_metadata_provider(),
            ),
        )

    @singleton_utils.singleton_class_method_result
    async def _get_temporal_worker(
        self,
    ) -> temporalio.worker.Worker:
        return worker.create_worker(
            task_queue=self.settings.temporal_worker.task_queue,
            client=await self._get_temporal_client(),
            workflows=await self._get_temporal_workflows(),
            activities=await self._get_temporal_activities(),
        )

    @abc.abstractmethod
    @singleton_utils.singleton_class_method_result
    async def _get_temporal_workflows(
        self,
    ) -> list[type[base.WorkflowProtocol]]:
        ...

    @abc.abstractmethod
    @singleton_utils.singleton_class_method_result
    async def _get_temporal_activities(
        self,
    ) -> list[base.ActivityProtocol]:
        ...

    @abc.abstractmethod
    @singleton_utils.singleton_class_method_result
    async def _get_temporal_client_metadata_provider(
        self,
    ) -> client.MetadataProvider:
        ...
