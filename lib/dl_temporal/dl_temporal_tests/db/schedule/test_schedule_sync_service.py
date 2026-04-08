import asyncio
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
from dl_temporal.schedule.config import (
    ScheduleConfig,
    TemporalSchedulesDynConfig,
)
from dl_temporal.schedule.services import ScheduleSyncService


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


def _schedule_data(schedule: ScheduleConfig) -> dict:
    return {
        "NAME": schedule.NAME,
        "WORKFLOW_NAME": schedule.WORKFLOW_NAME,
        "TASK_QUEUE": schedule.TASK_QUEUE,
        "INTERVAL": int(schedule.INTERVAL.total_seconds()),
    }


@pytest_asyncio.fixture(autouse=True)
async def fixture_cleanup_schedules(
    temporal_client: dl_temporal.TemporalClient,
) -> AsyncGenerator[None, None]:
    yield
    async for entry in await temporal_client.list_schedules():
        await temporal_client.delete_schedule(entry.id)


@pytest.fixture(name="config")
def fixture_config() -> TemporalSchedulesDynConfig:
    source = dl_dynconfig.InMemorySource(data={"SYNC_INTERVAL": 60, "SCHEDULES": []})
    return TemporalSchedulesDynConfig.model_from_source(source=source)


@pytest.fixture(name="service")
def fixture_service(
    temporal_client: dl_temporal.TemporalClient,
    config: TemporalSchedulesDynConfig,
) -> ScheduleSyncService:
    return ScheduleSyncService(
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
    service: ScheduleSyncService,
) -> None:
    await service.sync()
    await asyncio.sleep(1)  # wait for the schedule to be created

    ids = await _list_schedule_ids(temporal_client)
    assert _META_SCHEDULE_ID in ids


@pytest.mark.asyncio
async def test_sync_creates_configured_schedule(
    temporal_client: dl_temporal.TemporalClient,
    config: TemporalSchedulesDynConfig,
    service: ScheduleSyncService,
) -> None:
    schedule_id = f"system.test-configured-{uuid.uuid4()}"
    schedule = ScheduleConfig(
        NAME=schedule_id.removeprefix("system."),
        WORKFLOW_NAME=_TestWorkflow.name,
        TASK_QUEUE=_TEST_TASK_QUEUE,
        INTERVAL=dl_pydantic.JsonableTimedelta(seconds=300),
    )
    config.SCHEDULES = [schedule]
    await config.model_store()

    await service.sync()
    await asyncio.sleep(1)  # wait for the schedule to be created

    ids = await _list_schedule_ids(temporal_client)
    assert _META_SCHEDULE_ID in ids
    assert schedule_id in ids


@pytest.mark.asyncio
async def test_sync_updates_schedule(
    temporal_client: dl_temporal.TemporalClient,
    service: ScheduleSyncService,
    config: TemporalSchedulesDynConfig,
) -> None:
    schedule_id = f"system.test-update-{uuid.uuid4()}"
    name = schedule_id.removeprefix("system.")

    config.SCHEDULES = [
        ScheduleConfig(
            NAME=name,
            WORKFLOW_NAME=_TestWorkflow.name,
            TASK_QUEUE=_TEST_TASK_QUEUE,
            INTERVAL=dl_pydantic.JsonableTimedelta(seconds=300),
        )
    ]
    await config.model_store()
    await service.sync()
    await asyncio.sleep(1)  # wait for the schedule to be created

    config.SCHEDULES = [
        ScheduleConfig(
            NAME=name,
            WORKFLOW_NAME=_TestWorkflow.name,
            TASK_QUEUE=_TEST_TASK_QUEUE,
            INTERVAL=dl_pydantic.JsonableTimedelta(seconds=600),
        )
    ]
    await config.model_store()
    await service.sync()
    await asyncio.sleep(1)  # wait for the schedule to be created

    handle = await temporal_client.get_schedule(schedule_id)
    description = await handle.describe()
    assert len(description.schedule.spec.intervals) == 1
    assert description.schedule.spec.intervals[0].every == datetime.timedelta(seconds=600)


@pytest.mark.asyncio
async def test_sync_deletes_stale_schedule(
    temporal_client: dl_temporal.TemporalClient,
    service: ScheduleSyncService,
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
    await asyncio.sleep(1)  # wait for the schedule to be created

    ids = await _list_schedule_ids(temporal_client)
    assert stale_id not in ids


@pytest.mark.asyncio
async def test_sync_does_not_delete_non_prefixed_schedule(
    temporal_client: dl_temporal.TemporalClient,
    service: ScheduleSyncService,
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
    await asyncio.sleep(1)  # wait for the schedule to be created

    ids = await _list_schedule_ids(temporal_client)
    assert external_id in ids
