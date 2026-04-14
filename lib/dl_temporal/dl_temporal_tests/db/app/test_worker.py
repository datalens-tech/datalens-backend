import asyncio
import datetime
import uuid

import pytest
import temporalio.client

import dl_pydantic
import dl_temporal
import dl_temporal_tests.db.common as common
import dl_temporal_tests.db.workflows as workflows


@pytest.fixture(name="temporal_task_queue")
def fixture_temporal_task_queue() -> str:
    return "tests/db/app/test_worker"


@pytest.mark.asyncio
async def test_default(
    temporal_client: dl_temporal.TemporalClient,
    temporal_task_queue: str,
) -> None:
    random_id = uuid.uuid4()

    params = workflows.Workflow.Params(
        workflow_int_param=1,
        workflow_str_param="test",
        workflow_bool_param=True,
        workflow_list_param=[1, 2, 3],
        workflow_dict_param={"1": 1, "2": 2, "3": 3},
        workflow_timedelta_param=dl_pydantic.JsonableTimedelta(seconds=1),
        workflow_uuid_param=dl_pydantic.JsonableUUID(str(uuid.uuid4())),
        workflow_date_param=dl_pydantic.JsonableDate.today(),
        workflow_datetime_param=dl_pydantic.JsonableDatetime.now(tz=datetime.timezone.utc),
        workflow_datetime_with_timezone_param=dl_pydantic.JsonableDatetimeWithTimeZone.now(tz=datetime.timezone.utc),
        workflow_nested_param=common.NestedModel(test_int=1),
        parent_context=dl_temporal.ParentContext(request_id="parent_request_id"),
        return_error=False,
    )
    workflow_handler = await temporal_client.start_workflow(
        workflows.Workflow,
        params,
        id=str(random_id),
        task_queue=temporal_task_queue,
    )

    result: workflows.WorkflowResult = await workflow_handler.result()
    assert result.workflow_int_result == params.workflow_int_param + 3
    assert result.workflow_str_result == params.workflow_str_param
    assert result.workflow_bool_result == params.workflow_bool_param
    assert result.workflow_list_result == params.workflow_list_param
    assert result.workflow_dict_result == params.workflow_dict_param
    assert result.workflow_timedelta_result == params.workflow_timedelta_param
    assert result.workflow_uuid_result == params.workflow_uuid_param
    assert result.workflow_datetime_result == params.workflow_datetime_param
    assert result.workflow_datetime_with_timezone_result == params.workflow_datetime_with_timezone_param
    assert result.workflow_nested_result == common.NestedModel(test_int=params.workflow_nested_param.test_int + 3)

    workflow_desc = await workflow_handler.describe()
    assert workflow_desc.search_attributes.get(dl_temporal.base.SearchAttribute.RESULT_TYPE) == [
        dl_temporal.base.ResultType.SUCCESS,
    ]
    assert workflow_desc.search_attributes.get(dl_temporal.base.SearchAttribute.RESULT_CODE) is None


@pytest.mark.asyncio
async def test_error(
    temporal_client: dl_temporal.TemporalClient,
    temporal_task_queue: str,
) -> None:
    random_id = uuid.uuid4()

    params = workflows.Workflow.Params(
        workflow_int_param=1,
        workflow_str_param="test",
        workflow_bool_param=True,
        workflow_list_param=[1, 2, 3],
        workflow_dict_param={"1": 1, "2": 2, "3": 3},
        workflow_timedelta_param=dl_pydantic.JsonableTimedelta(seconds=1),
        workflow_uuid_param=dl_pydantic.JsonableUUID(str(uuid.uuid4())),
        workflow_date_param=dl_pydantic.JsonableDate.today(),
        workflow_datetime_param=dl_pydantic.JsonableDatetime.now(tz=datetime.timezone.utc),
        workflow_datetime_with_timezone_param=dl_pydantic.JsonableDatetimeWithTimeZone.now(tz=datetime.timezone.utc),
        workflow_nested_param=common.NestedModel(test_int=1),
        return_error=True,
    )
    workflow_handler = await temporal_client.start_workflow(
        workflows.Workflow,
        params,
        id=str(random_id),
        task_queue=temporal_task_queue,
    )

    result: workflows.WorkflowError = await workflow_handler.result()
    assert isinstance(result, workflows.WorkflowError)

    workflow_desc = await workflow_handler.describe()
    assert workflow_desc.search_attributes.get(dl_temporal.base.SearchAttribute.RESULT_TYPE) == [
        dl_temporal.base.ResultType.ERROR,
    ]
    assert workflow_desc.search_attributes.get(dl_temporal.base.SearchAttribute.RESULT_CODE) == [
        "ERROR_RESULT_CODE",
    ]


@pytest.mark.asyncio
async def test_list_schedule_executions(
    temporal_client: dl_temporal.TemporalClient,
    temporal_task_queue: str,
) -> None:
    schedule_id = str(uuid.uuid4())

    params = workflows.Workflow.Params(
        workflow_int_param=1,
        workflow_str_param="test",
        workflow_bool_param=True,
        workflow_list_param=[1, 2, 3],
        workflow_dict_param={"1": 1, "2": 2, "3": 3},
        workflow_timedelta_param=dl_pydantic.JsonableTimedelta(seconds=1),
        workflow_uuid_param=dl_pydantic.JsonableUUID(str(uuid.uuid4())),
        workflow_date_param=dl_pydantic.JsonableDate.today(),
        workflow_datetime_param=dl_pydantic.JsonableDatetime.now(tz=datetime.timezone.utc),
        workflow_datetime_with_timezone_param=dl_pydantic.JsonableDatetimeWithTimeZone.now(tz=datetime.timezone.utc),
        workflow_nested_param=common.NestedModel(test_int=1),
        parent_context=dl_temporal.ParentContext(request_id="test_list_schedule_executions"),
        return_error=False,
    )
    handle = await temporal_client.create_schedule(
        schedule_id=schedule_id,
        workflow=workflows.Workflow,
        params=params,
        task_queue=temporal_task_queue,
        spec=temporalio.client.ScheduleSpec(
            intervals=[temporalio.client.ScheduleIntervalSpec(every=datetime.timedelta(hours=1))],
        ),
    )

    await handle.trigger()

    try:
        executions: list[temporalio.api.workflow.v1.WorkflowExecutionInfo] = []
        deadline = asyncio.get_running_loop().time() + 10
        while True:
            executions, _ = await temporal_client.list_schedule_executions(schedule_id, page_size=10)
            if len(executions) >= 1:
                break
            if asyncio.get_running_loop().time() > deadline:
                pytest.fail("Timeout waiting for executions")
            await asyncio.sleep(0.1)

        assert len(executions) == 1
        assert executions[0].execution.workflow_id != ""
    finally:
        await handle.delete()
