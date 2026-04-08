import logging
from typing import Protocol

import attrs

import dl_temporal.base as base


LOGGER = logging.getLogger(__name__)


class _ScheduleSyncServiceProtocol(Protocol):
    async def sync(self) -> None:
        ...


class SyncSchedulesFromDynconfigActivityParams(base.BaseActivityParams):
    ...


class SyncSchedulesFromDynconfigActivityResult(base.BaseActivityResult):
    ...


@base.define_activity
@attrs.define(frozen=True, kw_only=True)
class SyncSchedulesFromDynconfigActivity(base.BaseActivity):
    name = "sync_schedules_from_dynconfig"
    logger = LOGGER
    Params = SyncSchedulesFromDynconfigActivityParams
    Result = SyncSchedulesFromDynconfigActivityResult

    schedule_sync_service: _ScheduleSyncServiceProtocol

    async def run(self, params: SyncSchedulesFromDynconfigActivityParams) -> SyncSchedulesFromDynconfigActivityResult:
        await self.schedule_sync_service.sync()
        return SyncSchedulesFromDynconfigActivityResult()
