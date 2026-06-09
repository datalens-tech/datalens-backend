import logging
import uuid

import pytest
import temporalio.client
import temporalio.worker

import dl_temporal
import dl_temporal.base
import dl_temporal_tests.db.workflows as workflows


@pytest.fixture(name="temporal_task_queue")
def fixture_temporal_task_queue() -> str:
    return "tests/db/app/test_logging"


def _workflow_log_lines(caplog: pytest.LogCaptureFixture) -> list[str]:
    # Resolved messages emitted by the workflow logger only (not every logger caplog captures).
    return [record.getMessage() for record in caplog.records if record.name == workflows.LOGGER.name]


@pytest.mark.asyncio
async def test_logs_completed_on_success(
    temporal_client: dl_temporal.TemporalClient,
    temporal_task_queue: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # request_id is set so ParentContextWorkflowMiddleware does not fill it from run_id, keeping the
    # logged params deterministic. from_default() uses fixed field values for the same reason.
    params = workflows.WorkflowParams.from_default(parent_context=dl_temporal.ParentContext(request_id="test_logging"))
    caplog.clear()
    with caplog.at_level(logging.DEBUG, logger=workflows.LOGGER.name):
        handle = await temporal_client.start_workflow(
            workflows.Workflow,
            params,
            id=str(uuid.uuid4()),
            task_queue=temporal_task_queue,
        )
        result = await handle.result()

    assert isinstance(result, workflows.WorkflowResult)
    assert _workflow_log_lines(caplog) == [
        'TemporalWorkflow(name=test_workflow).run: starting with params: {"execution_timeout":"PT1S","run_timeout":"PT10M","parent_close_policy":1,"parent_context":{"request_id":"test_logging","user_ip":null,"trace_id":null},"workflow_int_param":1,"workflow_str_param":"test","workflow_bool_param":true,"workflow_list_param":[1,2,3],"workflow_dict_param":{"1":1,"2":2,"3":3},"workflow_timedelta_param":"PT1S","workflow_uuid_param":"00000000-0000-0000-0000-000000000000","workflow_date_param":"2024-01-01","workflow_datetime_param":"2024-01-01T00:00:00.000000Z","workflow_datetime_with_timezone_param":"2024-01-01T00:00:00.000000Z","workflow_nested_param":{"test_int":1},"return_error":false}',
        'TemporalWorkflow(name=test_workflow).run: completed with result: {"type":"WorkflowResult","workflow_int_result":4,"workflow_str_result":"test","workflow_bool_result":true,"workflow_list_result":[1,2,3],"workflow_dict_result":{"1":1,"2":2,"3":3},"workflow_timedelta_result":"PT1S","workflow_uuid_result":"00000000-0000-0000-0000-000000000000","workflow_date_result":"2024-01-01","workflow_datetime_result":"2024-01-01T00:00:00.000000Z","workflow_datetime_with_timezone_result":"2024-01-01T00:00:00.000000Z","workflow_nested_result":{"test_int":4}}',
    ]


@pytest.mark.asyncio
async def test_logs_error_on_error_result(
    temporal_client: dl_temporal.TemporalClient,
    temporal_task_queue: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    params = workflows.WorkflowParams.from_default(
        return_error=True,
        parent_context=dl_temporal.ParentContext(request_id="test_logging"),
    )
    caplog.clear()
    with caplog.at_level(logging.DEBUG, logger=workflows.LOGGER.name):
        handle = await temporal_client.start_workflow(
            workflows.Workflow,
            params,
            id=str(uuid.uuid4()),
            task_queue=temporal_task_queue,
        )
        result = await handle.result()

    assert isinstance(result, workflows.WorkflowError)
    assert _workflow_log_lines(caplog) == [
        'TemporalWorkflow(name=test_workflow).run: starting with params: {"execution_timeout":"PT1S","run_timeout":"PT10M","parent_close_policy":1,"parent_context":{"request_id":"test_logging","user_ip":null,"trace_id":null},"workflow_int_param":1,"workflow_str_param":"test","workflow_bool_param":true,"workflow_list_param":[1,2,3],"workflow_dict_param":{"1":1,"2":2,"3":3},"workflow_timedelta_param":"PT1S","workflow_uuid_param":"00000000-0000-0000-0000-000000000000","workflow_date_param":"2024-01-01","workflow_datetime_param":"2024-01-01T00:00:00.000000Z","workflow_datetime_with_timezone_param":"2024-01-01T00:00:00.000000Z","workflow_nested_param":{"test_int":1},"return_error":true}',
        'TemporalWorkflow(name=test_workflow).run: finished with error: {"type":"WorkflowError"}',
    ]


@pytest.mark.asyncio
async def test_logs_failed_on_raise(
    temporal_client: dl_temporal.TemporalClient,
    temporal_task_queue: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    params = workflows.RaisingWorkflow.Params(parent_context=dl_temporal.ParentContext(request_id="test_logging"))
    caplog.clear()
    with caplog.at_level(logging.DEBUG, logger=workflows.LOGGER.name):
        handle = await temporal_client.start_workflow(
            workflows.RaisingWorkflow,
            params,
            id=str(uuid.uuid4()),
            task_queue=temporal_task_queue,
        )
        with pytest.raises(temporalio.client.WorkflowFailureError):
            await handle.result()

    assert _workflow_log_lines(caplog) == [
        'TemporalWorkflow(name=test_raising_workflow).run: starting with params: {"execution_timeout":"PT20M","run_timeout":"PT10M","parent_close_policy":1,"parent_context":{"request_id":"test_logging","user_ip":null,"trace_id":null}}',
        "TemporalWorkflow(name=test_raising_workflow).run: failed",
    ]


@pytest.mark.asyncio
async def test_skips_logs_on_replay(
    temporal_client: dl_temporal.TemporalClient,
    temporal_task_queue: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Run once against the worker to produce a full recorded history, then replay it.
    handle = await temporal_client.start_workflow(
        workflows.Workflow,
        workflows.WorkflowParams.from_default(),
        id=str(uuid.uuid4()),
        task_queue=temporal_task_queue,
    )
    await handle.result()
    history = await handle.fetch_history()

    # Replay the recorded history with the same workflow middlewares the worker uses. During a
    # pure replay is_replaying() is True throughout, so LoggingWorkflowMiddleware must emit nothing.
    replayer = temporalio.worker.Replayer(
        workflows=[workflows.Workflow],
        interceptors=[
            dl_temporal.TemporalInterceptor(
                workflow_middlewares=(
                    dl_temporal.ParentContextWorkflowMiddleware(),
                    dl_temporal.SearchAttributesWorkflowMiddleware(),
                    dl_temporal.LoggingWorkflowMiddleware(),
                ),
                activity_middlewares=(),
            ),
        ],
        data_converter=dl_temporal.base.DataConverter(),
    )

    caplog.clear()
    with caplog.at_level(logging.DEBUG, logger=workflows.LOGGER.name):
        await replayer.replay_workflow(history)

    assert _workflow_log_lines(caplog) == []
