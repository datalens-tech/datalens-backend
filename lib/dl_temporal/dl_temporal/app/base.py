from typing import (
    Generic,
    TypeVar,
)

import attr
import pydantic
from typing_extensions import override

import dl_app_api_base
import dl_app_base
import dl_temporal.app.temporal as temporal_app
import dl_temporal.base as base
import dl_temporal.schedule.config as schedule_config
import dl_temporal.schedule.services as schedule_services
import dl_temporal.temporal.activities as temporal_activities
import dl_temporal.temporal.workflows as temporal_workflows


class BaseTemporalWorkerAppSettings(
    temporal_app.TemporalWorkerAppSettingsMixin,
    dl_app_api_base.HttpServerAppSettingsMixin,
    dl_app_base.BaseAppSettings,
):
    ...


class BaseTemporalWorkerAppDynconfigMixin(dl_app_api_base.HttpServerAppDynconfigMixin):
    TEMPORAL_SCHEDULES: schedule_config.TemporalSchedulesDynConfig = pydantic.Field(
        default_factory=schedule_config.TemporalSchedulesDynConfig
    )


@attr.define(frozen=True, kw_only=True)
class BaseTemporalWorkerApp(
    temporal_app.TemporalWorkerAppMixin,
    dl_app_api_base.HttpServerAppMixin,
    dl_app_base.BaseApp,
):
    ...


AppType = TypeVar("AppType", bound=BaseTemporalWorkerApp)


@attr.define(kw_only=True, slots=False)
class BaseTemporalWorkerAppFactory(
    temporal_app.TemporalWorkerAppFactoryMixin[AppType],
    dl_app_api_base.HttpServerAppFactoryMixin[AppType],
    dl_app_base.BaseAppFactory[AppType],
    Generic[AppType],
):
    settings: BaseTemporalWorkerAppSettings

    @override
    @dl_app_base.singleton_class_method_result
    async def _get_dynconfig(self) -> BaseTemporalWorkerAppDynconfigMixin:
        return BaseTemporalWorkerAppDynconfigMixin.model_from_source(
            source=await self._get_dynconfig_source(),
        )

    @dl_app_base.singleton_class_method_result
    async def _get_schedule_sync_service(self) -> schedule_services.ScheduleSyncService:
        return schedule_services.ScheduleSyncService(
            temporal_client=await self._get_temporal_client(),
            config=(await self._get_dynconfig()).TEMPORAL_SCHEDULES,
            task_queue=self.settings.TEMPORAL_WORKER.TASK_QUEUE,
            workflows=await self._get_temporal_workflows(),
        )

    @override
    @dl_app_base.singleton_class_method_result
    async def _get_startup_callbacks(self) -> list[dl_app_base.Callback]:
        result = await super()._get_startup_callbacks()
        schedule_sync_service = await self._get_schedule_sync_service()
        result.append(
            dl_app_base.Callback(
                name="schedule_sync_service.sync",
                coroutine=schedule_sync_service.sync(),
            )
        )
        return result

    @override
    @dl_app_base.singleton_class_method_result
    async def _get_temporal_activities(self) -> list[base.ActivityProtocol]:
        return [
            *await super()._get_temporal_activities(),
            temporal_activities.SyncSchedulesFromDynconfigActivity(
                schedule_sync_service=await self._get_schedule_sync_service()
            ),
        ]

    @override
    @dl_app_base.singleton_class_method_result
    async def _get_temporal_workflows(self) -> list[type[base.WorkflowProtocol]]:
        return [
            *await super()._get_temporal_workflows(),
            temporal_workflows.SyncSchedulesFromDynconfigWorkflow,
        ]

    @override
    @dl_app_base.singleton_class_method_result
    async def _get_aiohttp_subsystem_readiness_callbacks(
        self,
    ) -> list[dl_app_api_base.SubsystemReadinessCallback]:
        result = await super()._get_aiohttp_subsystem_readiness_callbacks()

        temporal_client = await self._get_temporal_client()
        result.append(
            dl_app_api_base.SubsystemReadinessAsyncCallback(
                name="temporal_client.check_health",
                is_ready=temporal_client.check_health,
            ),
        )

        temporal_worker = await self._get_temporal_worker()
        result.append(
            dl_app_api_base.SubsystemReadinessSyncCallback(
                name="temporal_worker.is_running",
                is_ready=lambda: temporal_worker.is_running,
            ),
        )

        return result
