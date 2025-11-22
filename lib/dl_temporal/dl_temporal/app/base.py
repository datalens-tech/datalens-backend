from typing import (
    Generic,
    TypeVar,
)

import attr
from typing_extensions import override

import dl_app_api_base
import dl_app_base
import dl_temporal.app.temporal as temporal_app


class BaseTemporalWorkerAppSettings(
    temporal_app.TemporalWorkerAppSettingsMixin,
    dl_app_api_base.HttpServerAppSettingsMixin,
    dl_app_base.BaseAppSettings,
):
    ...


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
