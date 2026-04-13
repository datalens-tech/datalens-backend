import datetime
import logging
from typing import AsyncGenerator
import uuid

import pytest
import pytest_asyncio
import temporalio.client

import dl_dynconfig
import dl_pydantic
import dl_temporal
import dl_temporal.schedule
import dl_temporal_tests.db.common as common


LOGGER = logging.getLogger(__name__)


class _TestWorkflowParams(dl_temporal.BaseWorkflowParams):
    ...


class _TestWorkflowResult(dl_temporal.BaseWorkflowResult):
    ...


class _TestWorkflow(dl_temporal.BaseWorkflow):
    name = "test_schedule_sync_service_workflow"
    Params = _TestWorkflowParams
    Result = _TestWorkflowResult
    logger = LOGGER

    async def run(self, params: _TestWorkflowParams) -> _TestWorkflowResult:
        return _TestWorkflowResult()


_TEST_TASK_QUEUE = "tests/db/schedule/sync_service"
_META_SCHEDULE_ID = "system.sync_schedules_from_dynconfig"


@pytest_asyncio.fixture(autouse=True)
async def fixture_cleanup_schedules(
    temporal_client: dl_temporal.TemporalClient,
) -> AsyncGenerator[None, None]:
    async for entry in await temporal_client.list_schedules():
        try:
            await temporal_client.delete_schedule(entry.id)
        except temporalio.service.RPCError:
            pass
    yield


@pytest.fixture(name="config")
def fixture_config() -> dl_temporal.schedule.TemporalSchedulesDynConfig:
    source = dl_dynconfig.InMemorySource(data={"SYNC_INTERVAL": 60, "SCHEDULES": []})
    return dl_temporal.schedule.TemporalSchedulesDynConfig.model_from_source(source=source)


@pytest.fixture(name="service")
def fixture_service(
    temporal_client: dl_temporal.TemporalClient,
    config: dl_temporal.schedule.TemporalSchedulesDynConfig,
) -> dl_temporal.schedule.ScheduleSyncService:
    return dl_temporal.schedule.ScheduleSyncService(
        temporal_client=temporal_client,
        config=config,
        task_queue=_TEST_TASK_QUEUE,
        workflows=[_TestWorkflow],
    )


async def _list_schedule_ids(temporal_client: dl_temporal.TemporalClient) -> set[str]:
    return {entry.id async for entry in await temporal_client.list_schedules()}


@pytest.mark.asyncio
async def test_sync_creates_meta_schedule(
    temporal_client: dl_temporal.TemporalClient,
    service: dl_temporal.schedule.ScheduleSyncService,
) -> None:
    await service.sync()

    async def _meta_visible() -> None:
        assert _META_SCHEDULE_ID in await _list_schedule_ids(temporal_client)

    await common.await_for_success(
        f"{_META_SCHEDULE_ID} visible in schedule list",
        _meta_visible,
    )


@pytest.mark.asyncio
async def test_sync_creates_configured_schedule(
    temporal_client: dl_temporal.TemporalClient,
    config: dl_temporal.schedule.TemporalSchedulesDynConfig,
    service: dl_temporal.schedule.ScheduleSyncService,
) -> None:
    schedule_id = f"system.test-configured-{uuid.uuid4()}"
    schedule = dl_temporal.schedule.ScheduleConfig(
        NAME=schedule_id.removeprefix("system."),
        WORKFLOW_NAME=_TestWorkflow.name,
        TASK_QUEUE=_TEST_TASK_QUEUE,
        INTERVAL=dl_pydantic.JsonableTimedelta(seconds=300),
    )
    config.SCHEDULES = [schedule]
    await config.model_store()

    await service.sync()

    async def _schedules_visible() -> None:
        ids = await _list_schedule_ids(temporal_client)
        assert _META_SCHEDULE_ID in ids
        assert schedule_id in ids

    await common.await_for_success(
        "configured schedules visible in schedule list",
        _schedules_visible,
    )


@pytest.mark.asyncio
async def test_sync_updates_schedule(
    temporal_client: dl_temporal.TemporalClient,
    service: dl_temporal.schedule.ScheduleSyncService,
    config: dl_temporal.schedule.TemporalSchedulesDynConfig,
) -> None:
    schedule_id = f"system.test-update-{uuid.uuid4()}"
    name = schedule_id.removeprefix("system.")

    config.SCHEDULES = [
        dl_temporal.schedule.ScheduleConfig(
            NAME=name,
            WORKFLOW_NAME=_TestWorkflow.name,
            TASK_QUEUE=_TEST_TASK_QUEUE,
            INTERVAL=dl_pydantic.JsonableTimedelta(seconds=300),
        )
    ]
    await config.model_store()
    await service.sync()

    async def _schedule_visible() -> None:
        assert schedule_id in await _list_schedule_ids(temporal_client)

    await common.await_for_success(f"{schedule_id} visible in schedule list", _schedule_visible)

    config.SCHEDULES = [
        dl_temporal.schedule.ScheduleConfig(
            NAME=name,
            WORKFLOW_NAME=_TestWorkflow.name,
            TASK_QUEUE=_TEST_TASK_QUEUE,
            INTERVAL=dl_pydantic.JsonableTimedelta(seconds=600),
        )
    ]
    await config.model_store()
    await service.sync()

    async def _interval_updated() -> None:
        handle = await temporal_client.get_schedule(schedule_id)
        description = await handle.describe()
        assert len(description.schedule.spec.intervals) == 1
        assert description.schedule.spec.intervals[0].every == datetime.timedelta(seconds=600)

    await common.await_for_success(f"{schedule_id} interval updated to 600s", _interval_updated)


@pytest.mark.asyncio
async def test_sync_deletes_stale_schedule(
    temporal_client: dl_temporal.TemporalClient,
    service: dl_temporal.schedule.ScheduleSyncService,
) -> None:
    stale_id = f"system.stale-{uuid.uuid4()}"
    await temporal_client.create_schedule(
        schedule_id=stale_id,
        workflow=_TestWorkflow,
        params=_TestWorkflowParams(),
        task_queue=_TEST_TASK_QUEUE,
        spec=temporalio.client.ScheduleSpec(
            intervals=[temporalio.client.ScheduleIntervalSpec(every=datetime.timedelta(hours=1))],
        ),
    )

    await service.sync()

    async def _stale_removed() -> None:
        assert stale_id not in await _list_schedule_ids(temporal_client)

    await common.await_for_success(f"{stale_id} removed from schedule list", _stale_removed)


@pytest.mark.asyncio
async def test_sync_disabled_creates_only_meta_schedule(
    temporal_client: dl_temporal.TemporalClient,
    service: dl_temporal.schedule.ScheduleSyncService,
    config: dl_temporal.schedule.TemporalSchedulesDynConfig,
) -> None:
    schedule_id = f"system.test-disabled-{uuid.uuid4()}"
    config.ENABLED = False
    config.SCHEDULES = [
        dl_temporal.schedule.ScheduleConfig(
            NAME=schedule_id.removeprefix("system."),
            WORKFLOW_NAME=_TestWorkflow.name,
            TASK_QUEUE=_TEST_TASK_QUEUE,
            INTERVAL=dl_pydantic.JsonableTimedelta(seconds=300),
        )
    ]
    await config.model_store()

    await service.sync()

    async def _schedule_not_visible() -> None:
        schedule_ids = await _list_schedule_ids(temporal_client)
        assert schedule_id not in schedule_ids
        assert _META_SCHEDULE_ID in schedule_ids

    await common.await_for_success(
        f"{schedule_id} not visible and {_META_SCHEDULE_ID} visible in schedule list",
        _schedule_not_visible,
    )


@pytest.mark.asyncio
async def test_sync_pauses_schedule(
    temporal_client: dl_temporal.TemporalClient,
    service: dl_temporal.schedule.ScheduleSyncService,
    config: dl_temporal.schedule.TemporalSchedulesDynConfig,
) -> None:
    schedule_id = f"system.test-paused-{uuid.uuid4()}"
    config.SCHEDULES = [
        dl_temporal.schedule.ScheduleConfig(
            NAME=schedule_id.removeprefix("system."),
            WORKFLOW_NAME=_TestWorkflow.name,
            TASK_QUEUE=_TEST_TASK_QUEUE,
            INTERVAL=dl_pydantic.JsonableTimedelta(seconds=300),
            PAUSED=True,
        )
    ]
    await config.model_store()

    await service.sync()

    async def _schedule_paused() -> None:
        handle = await temporal_client.get_schedule(schedule_id)
        description = await handle.describe()
        assert description.schedule.state.paused

    await common.await_for_success(f"{schedule_id} paused", _schedule_paused)


@pytest.mark.asyncio
async def test_sync_unpauses_schedule(
    temporal_client: dl_temporal.TemporalClient,
    service: dl_temporal.schedule.ScheduleSyncService,
    config: dl_temporal.schedule.TemporalSchedulesDynConfig,
) -> None:
    schedule_id = f"system.test-unpaused-{uuid.uuid4()}"
    config.SCHEDULES = [
        dl_temporal.schedule.ScheduleConfig(
            NAME=schedule_id.removeprefix("system."),
            WORKFLOW_NAME=_TestWorkflow.name,
            TASK_QUEUE=_TEST_TASK_QUEUE,
            INTERVAL=dl_pydantic.JsonableTimedelta(seconds=300),
            PAUSED=True,
        )
    ]
    await config.model_store()
    await service.sync()

    async def _schedule_paused() -> None:
        handle = await temporal_client.get_schedule(schedule_id)
        description = await handle.describe()
        assert description.schedule.state.paused

    await common.await_for_success(f"{schedule_id} paused", _schedule_paused)

    config.SCHEDULES = [
        dl_temporal.schedule.ScheduleConfig(
            NAME=schedule_id.removeprefix("system."),
            WORKFLOW_NAME=_TestWorkflow.name,
            TASK_QUEUE=_TEST_TASK_QUEUE,
            INTERVAL=dl_pydantic.JsonableTimedelta(seconds=300),
            PAUSED=False,
        )
    ]
    await config.model_store()
    await service.sync()

    async def _schedule_unpaused() -> None:
        handle = await temporal_client.get_schedule(schedule_id)
        description = await handle.describe()
        assert not description.schedule.state.paused

    await common.await_for_success(f"{schedule_id} unpaused", _schedule_unpaused)


@pytest.mark.asyncio
async def test_sync_does_not_delete_non_prefixed_schedule(
    temporal_client: dl_temporal.TemporalClient,
    service: dl_temporal.schedule.ScheduleSyncService,
) -> None:
    external_id = f"external-{uuid.uuid4()}"
    await temporal_client.create_schedule(
        schedule_id=external_id,
        workflow=_TestWorkflow,
        params=_TestWorkflowParams(),
        task_queue=_TEST_TASK_QUEUE,
        spec=temporalio.client.ScheduleSpec(
            intervals=[temporalio.client.ScheduleIntervalSpec(every=datetime.timedelta(hours=1))],
        ),
    )

    await service.sync()

    async def _sync_completed() -> None:
        ids = await _list_schedule_ids(temporal_client)
        assert _META_SCHEDULE_ID in ids
        assert external_id in ids

    await common.await_for_success(
        f"{_META_SCHEDULE_ID} visible and {external_id} preserved after sync",
        _sync_completed,
    )


@pytest.mark.asyncio
async def test_sync_creates_schedule_with_phase(
    temporal_client: dl_temporal.TemporalClient,
    config: dl_temporal.schedule.TemporalSchedulesDynConfig,
    service: dl_temporal.schedule.ScheduleSyncService,
) -> None:
    schedule_id = f"system.test-phase-{uuid.uuid4()}"
    config.SCHEDULES = [
        dl_temporal.schedule.ScheduleConfig(
            NAME=schedule_id.removeprefix("system."),
            WORKFLOW_NAME=_TestWorkflow.name,
            TASK_QUEUE=_TEST_TASK_QUEUE,
            INTERVAL=dl_pydantic.JsonableTimedelta(seconds=300),
            PHASE=dl_pydantic.JsonableTimedelta(seconds=60),
        )
    ]
    await config.model_store()

    await service.sync()

    async def _schedule_offset_set() -> None:
        handle = await temporal_client.get_schedule(schedule_id)
        description = await handle.describe()
        assert len(description.schedule.spec.intervals) == 1
        assert description.schedule.spec.intervals[0].offset == datetime.timedelta(seconds=60)

    await common.await_for_success(f"{schedule_id} created with phase offset", _schedule_offset_set)
