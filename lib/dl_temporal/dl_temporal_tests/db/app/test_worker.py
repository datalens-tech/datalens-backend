import datetime
import uuid

import pytest

import dl_pydantic
import dl_temporal
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
