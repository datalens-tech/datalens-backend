import datetime
import logging
from typing import AsyncGenerator
import uuid

import pytest
import pytest_asyncio
import temporalio.worker

import dl_pydantic
import dl_temporal
import dl_temporal.testing as dl_temporal_testing
import dl_temporal_tests.db.worker.activities as activities
import dl_temporal_tests.db.worker.workflows as workflows
import dl_testing


@pytest.fixture(name="temporal_queue_name")
def fixture_temporal_queue_name() -> str:
    return "test_queue_name"


@pytest_asyncio.fixture(name="temporal_worker", autouse=True)
async def fixture_temporal_worker(
    temporal_client: dl_temporal.TemporalClient,
    temporal_queue_name: str,
    temporal_ui_hostport: dl_testing.HostPort,
) -> AsyncGenerator[temporalio.worker.Worker, None]:
    worker = dl_temporal.create_worker(
        task_queue=temporal_queue_name,
        client=temporal_client,
        workflows=[workflows.Workflow],
        activities=[activities.Activity()],
    )
    logging.info(f"Temporal UI URL: http://{temporal_ui_hostport.host}:{temporal_ui_hostport.port}")

    async with dl_temporal_testing.worker_run_context(worker=worker) as worker:
        yield worker


@pytest.mark.asyncio
async def test_default(
    temporal_client: dl_temporal.TemporalClient,
    temporal_queue_name: str,
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
    )
    workflow_handler = await temporal_client.start_workflow(
        workflows.Workflow,
        params,
        id=str(random_id),
        task_queue=temporal_queue_name,
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
