from typing import (
    Generic,
    TypeVar,
)

import attr
from typing_extensions import override

import dl_temporal.app.aiohttp as aiohttp_app
import dl_temporal.app.temporal as temporal_app
import dl_temporal.utils.aiohttp as aiohttp_utils
import dl_temporal.utils.app as app_utils
import dl_temporal.utils.singleton as singleton_utils


class BaseTemporalWorkerAppSettings(
    temporal_app.TemporalWorkerAppSettingsMixin,
    aiohttp_app.HttpServerAppSettingsMixin,
    app_utils.BaseAppSettings,
):
    ...


@attr.define(frozen=True, kw_only=True)
class BaseTemporalWorkerApp(
    temporal_app.TemporalWorkerAppMixin,
    aiohttp_app.HttpServerAppMixin,
    app_utils.BaseApp,
):
    ...


AppType = TypeVar("AppType", bound=BaseTemporalWorkerApp)


@attr.define(kw_only=True, slots=False)
class BaseTemporalWorkerAppFactory(
    temporal_app.TemporalWorkerAppFactoryMixin[AppType],
    aiohttp_app.HttpServerAppFactoryMixin[AppType],
    app_utils.BaseAppFactory[AppType],
    Generic[AppType],
):
    settings: BaseTemporalWorkerAppSettings

    @override
    @singleton_utils.singleton_class_method_result
    async def _get_aiohttp_subsystem_readiness_callbacks(
        self,
    ) -> list[aiohttp_utils.SubsystemReadinessCallback]:
        result = await super()._get_aiohttp_subsystem_readiness_callbacks()

        temporal_client = await self._get_temporal_client()
        result.append(
            aiohttp_utils.SubsystemReadinessAsyncCallback(
                name="temporal_client.check_health",
                is_ready=temporal_client.check_health,
            ),
        )

        temporal_worker = await self._get_temporal_worker()
        result.append(
            aiohttp_utils.SubsystemReadinessSyncCallback(
                name="temporal_worker.is_running",
                is_ready=lambda: temporal_worker.is_running,
            ),
        )

        return result
