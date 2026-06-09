import asyncio
import datetime
import uuid

import pytest
import temporalio.client

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

    params = workflows.WorkflowParams.from_default(
        parent_context=dl_temporal.ParentContext(request_id="parent_request_id"),
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

    params = workflows.WorkflowParams.from_default(return_error=True)
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
async def test_parent_context_middleware_fills_request_id_when_unset(
    temporal_client: dl_temporal.TemporalClient,
    temporal_task_queue: str,
) -> None:
    # ParentContext has no request_id — ParentContextWorkflowMiddleware fills it
    # from temporalio.workflow.info().run_id before the workflow body runs. The
    # activity asserts `params.parent_context.request_id is not None`; reaching
    # here without WorkflowError proves the middleware fired through the
    # TemporalInterceptor.
    params = workflows.WorkflowParams.from_default(parent_context=dl_temporal.ParentContext())
    workflow_handler = await temporal_client.start_workflow(
        workflows.Workflow,
        params,
        id=str(uuid.uuid4()),
        task_queue=temporal_task_queue,
    )
    result = await workflow_handler.result()
    assert isinstance(result, workflows.WorkflowResult)


@pytest.mark.asyncio
async def test_list_schedule_executions(
    temporal_client: dl_temporal.TemporalClient,
    temporal_task_queue: str,
) -> None:
    schedule_id = str(uuid.uuid4())

    params = workflows.WorkflowParams.from_default(
        parent_context=dl_temporal.ParentContext(request_id="test_list_schedule_executions"),
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

    try:
        await temporal_client.trigger_schedule(
            schedule_id=schedule_id,
            overlap=temporalio.client.ScheduleOverlapPolicy.ALLOW_ALL,
        )

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
