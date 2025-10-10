import datetime
import logging
from typing import AsyncGenerator
import uuid

import pydantic
import pytest
import pytest_asyncio
import temporalio.worker

import dl_temporal
import dl_temporal.testing as dl_temporal_testing
import dl_testing


class ActivityParams(dl_temporal.BaseActivityParams):
    activity_int_param: int
    activity_str_param: str
    activity_bool_param: bool
    activity_list_param: list[int]
    activity_dict_param: dict[str, int]
    activity_timedelta_param: datetime.timedelta
    activity_uuid_param: uuid.UUID
    activity_datetime_param: pydantic.AwareDatetime

    start_to_close_timeout: datetime.timedelta = datetime.timedelta(seconds=30)


class ActivityResult(dl_temporal.BaseActivityResult):
    activity_int_result: int
    activity_str_result: str
    activity_bool_result: bool
    activity_list_result: list[int]
    activity_dict_result: dict[str, int]
    activity_timedelta_result: datetime.timedelta
    activity_uuid_result: uuid.UUID
    activity_datetime_result: pydantic.AwareDatetime


@dl_temporal.define_activity
class Activity(dl_temporal.BaseActivity):
    name = "test_activity"
    Params = ActivityParams
    Result = ActivityResult

    async def run(self, params: ActivityParams) -> ActivityResult:
        return self.Result(
            activity_int_result=params.activity_int_param + 1,
            activity_str_result=params.activity_str_param,
            activity_bool_result=params.activity_bool_param,
            activity_list_result=params.activity_list_param,
            activity_dict_result=params.activity_dict_param,
            activity_timedelta_result=params.activity_timedelta_param,
            activity_uuid_result=params.activity_uuid_param,
            activity_datetime_result=params.activity_datetime_param,
        )


class WorkflowParams(dl_temporal.BaseWorkflowParams):
    workflow_int_param: int
    workflow_str_param: str
    workflow_bool_param: bool
    workflow_list_param: list[int]
    workflow_dict_param: dict[str, int]
    workflow_timedelta_param: datetime.timedelta
    workflow_uuid_param: uuid.UUID
    workflow_datetime_param: pydantic.AwareDatetime

    execution_timeout: datetime.timedelta = datetime.timedelta(seconds=1)


class WorkflowResult(dl_temporal.BaseWorkflowResult):
    workflow_int_result: int
    workflow_str_result: str
    workflow_bool_result: bool
    workflow_list_result: list[int]
    workflow_dict_result: dict[str, int]
    workflow_timedelta_result: datetime.timedelta
    workflow_uuid_result: uuid.UUID
    workflow_datetime_result: pydantic.AwareDatetime


@dl_temporal.define_workflow
class Workflow(dl_temporal.BaseWorkflow):
    name = "test_workflow"
    Params = WorkflowParams
    Result = WorkflowResult

    async def run(self, params: WorkflowParams) -> WorkflowResult:
        result = await self.execute_activity(
            Activity,
            Activity.Params(
                activity_int_param=params.workflow_int_param + 1,
                activity_str_param=params.workflow_str_param,
                activity_bool_param=params.workflow_bool_param,
                activity_list_param=params.workflow_list_param,
                activity_dict_param=params.workflow_dict_param,
                activity_timedelta_param=params.workflow_timedelta_param,
                activity_uuid_param=params.workflow_uuid_param,
                activity_datetime_param=params.workflow_datetime_param,
            ),
        )
        return self.Result(
            workflow_int_result=result.activity_int_result + 1,
            workflow_str_result=result.activity_str_result,
            workflow_bool_result=result.activity_bool_result,
            workflow_list_result=result.activity_list_result,
            workflow_dict_result=result.activity_dict_result,
            workflow_timedelta_result=result.activity_timedelta_result,
            workflow_uuid_result=result.activity_uuid_result,
            workflow_datetime_result=result.activity_datetime_result,
        )


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
        workflows=[Workflow],
        activities=[Activity()],
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

    params = Workflow.Params(
        workflow_int_param=1,
        workflow_str_param="test",
        workflow_bool_param=True,
        workflow_list_param=[1, 2, 3],
        workflow_dict_param={"1": 1, "2": 2, "3": 3},
        workflow_timedelta_param=datetime.timedelta(seconds=1),
        workflow_uuid_param=uuid.uuid4(),
        workflow_datetime_param=datetime.datetime.now(tz=datetime.timezone.utc),
    )
    workflow_handler = await temporal_client.start_workflow(
        Workflow,
        params,
        id=str(random_id),
        task_queue=temporal_queue_name,
    )

    result = await workflow_handler.result()
    assert result.workflow_int_result == params.workflow_int_param + 3
    assert result.workflow_str_result == params.workflow_str_param
    assert result.workflow_bool_result == params.workflow_bool_param
    assert result.workflow_list_result == params.workflow_list_param
    assert result.workflow_dict_result == params.workflow_dict_param
    assert result.workflow_timedelta_result == params.workflow_timedelta_param
    assert result.workflow_uuid_result == params.workflow_uuid_param
    assert result.workflow_datetime_result == params.workflow_datetime_param
