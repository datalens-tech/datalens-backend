import datetime
import uuid

import pytest
import temporalio.client
import temporalio.service

import dl_pydantic
import dl_temporal
import dl_temporal_tests.db.workflows as workflows


@pytest.fixture(name="temporal_task_queue")
def fixture_temporal_task_queue() -> str:
    return "tests/db/client/test_schedule"


@pytest.fixture(name="workflow_params")
def fixture_workflow_params() -> workflows.WorkflowParams:
    return workflows.WorkflowParams(
        workflow_int_param=1,
        workflow_str_param="test",
        workflow_bool_param=True,
        workflow_list_param=[1, 2, 3],
        workflow_dict_param={"a": 1},
        workflow_timedelta_param=dl_pydantic.JsonableTimedelta(seconds=1),
        workflow_uuid_param=dl_pydantic.JsonableUUID(str(uuid.uuid4())),
        workflow_date_param=dl_pydantic.JsonableDate.today(),
        workflow_datetime_param=dl_pydantic.JsonableDatetime.now(tz=datetime.timezone.utc),
        workflow_datetime_with_timezone_param=dl_pydantic.JsonableDatetimeWithTimeZone.now(tz=datetime.timezone.utc),
    )


@pytest.fixture(name="schedule_spec")
def fixture_schedule_spec() -> temporalio.client.ScheduleSpec:
    return temporalio.client.ScheduleSpec(
        intervals=[temporalio.client.ScheduleIntervalSpec(every=datetime.timedelta(hours=1))],
    )


@pytest.mark.asyncio
async def test_create_schedule(
    temporal_client: dl_temporal.TemporalClient,
    temporal_task_queue: str,
    workflow_params: workflows.WorkflowParams,
    schedule_spec: temporalio.client.ScheduleSpec,
) -> None:
    schedule_id = f"test-create-{uuid.uuid4()}"

    try:
        handle = await temporal_client.create_schedule(
            schedule_id=schedule_id,
            workflow=workflows.Workflow,
            params=workflow_params,
            task_queue=temporal_task_queue,
            spec=schedule_spec,
        )
        assert handle is not None
        assert handle.id == schedule_id
    finally:
        await temporal_client.delete_schedule(schedule_id)


@pytest.mark.asyncio
async def test_get_schedule(
    temporal_client: dl_temporal.TemporalClient,
    temporal_task_queue: str,
    workflow_params: workflows.WorkflowParams,
    schedule_spec: temporalio.client.ScheduleSpec,
) -> None:
    schedule_id = f"test-get-{uuid.uuid4()}"

    try:
        await temporal_client.create_schedule(
            schedule_id=schedule_id,
            workflow=workflows.Workflow,
            params=workflow_params,
            task_queue=temporal_task_queue,
            spec=schedule_spec,
        )

        handle = await temporal_client.get_schedule(schedule_id)
        assert handle is not None
        assert handle.id == schedule_id

        description = await handle.describe()
        assert description.id == schedule_id
    finally:
        await temporal_client.delete_schedule(schedule_id)


@pytest.mark.asyncio
async def test_update_schedule_spec(
    temporal_client: dl_temporal.TemporalClient,
    temporal_task_queue: str,
    workflow_params: workflows.WorkflowParams,
    schedule_spec: temporalio.client.ScheduleSpec,
) -> None:
    schedule_id = f"test-update-{uuid.uuid4()}"

    try:
        await temporal_client.create_schedule(
            schedule_id=schedule_id,
            workflow=workflows.Workflow,
            params=workflow_params,
            task_queue=temporal_task_queue,
            spec=schedule_spec,
        )

        new_spec = temporalio.client.ScheduleSpec(
            intervals=[temporalio.client.ScheduleIntervalSpec(every=datetime.timedelta(hours=2))],
        )
        await temporal_client.update_schedule_spec(schedule_id, new_spec)

        handle = await temporal_client.get_schedule(schedule_id)
        description = await handle.describe()
        assert len(description.schedule.spec.intervals) == 1
        assert description.schedule.spec.intervals[0].every == datetime.timedelta(hours=2)
    finally:
        await temporal_client.delete_schedule(schedule_id)


@pytest.mark.asyncio
async def test_delete_schedule(
    temporal_client: dl_temporal.TemporalClient,
    temporal_task_queue: str,
    workflow_params: workflows.WorkflowParams,
    schedule_spec: temporalio.client.ScheduleSpec,
) -> None:
    schedule_id = f"test-delete-{uuid.uuid4()}"

    await temporal_client.create_schedule(
        schedule_id=schedule_id,
        workflow=workflows.Workflow,
        params=workflow_params,
        task_queue=temporal_task_queue,
        spec=schedule_spec,
    )

    await temporal_client.delete_schedule(schedule_id)

    handle = await temporal_client.get_schedule(schedule_id)
    with pytest.raises(temporalio.service.RPCError):
        await handle.describe()
