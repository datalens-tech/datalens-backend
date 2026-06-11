import uuid

import aiohttp
import attrs
import pytest
import temporalio.client

import dl_temporal
import dl_temporal.app
import dl_temporal_tests.db.workflows as workflows

# Execution series whose value is deterministic after a fixed number of executions: the counters and
# the histogram observation counts. The time-dependent _sum/_bucket series are intentionally excluded.
_FILTERED_METRIC_NAMES = frozenset(
    {
        "temporal_workflow_execution_total",
        "temporal_workflow_execution_duration_seconds_count",
        "temporal_activity_execution_total",
        "temporal_activity_execution_duration_seconds_count",
    }
)


@attrs.define(frozen=True)
class Metric:
    name: str
    labels: str
    value: float


@pytest.fixture(name="temporal_task_queue")
def fixture_temporal_task_queue() -> str:
    return "tests/db/app/test_metrics"


async def _scrape_metrics(
    app_client: aiohttp.ClientSession,
    app_settings: dl_temporal.app.BaseTemporalWorkerAppSettings,
    *entity_names: str,
) -> set[Metric]:
    # Split the exposition into lines and keep the execution-count samples for the given workflow and
    # activity names as a set of Metric, so a test can assert the whole filtered set with a single
    # equality. Filtering by name ignores unrelated workflows on the worker (e.g. the schedule-sync
    # system workflow) instead of disabling them.
    response = await app_client.get(app_settings.METRICS.PATH)
    assert response.status == 200
    body = await response.text()
    result: set[Metric] = set()
    for line in body.splitlines():
        if not line or line.startswith("#"):
            continue
        series, _, value = line.rpartition(" ")
        name, _, labels = series.partition("{")
        labels = labels.rstrip("}")
        if name not in _FILTERED_METRIC_NAMES:
            continue
        if not any(f'name="{entity}"' in labels for entity in entity_names):
            continue
        result.add(Metric(name=name, labels=labels, value=float(value)))
    return result


@pytest.mark.asyncio
async def test_metrics_recorded_on_success(
    temporal_client: dl_temporal.TemporalClient,
    temporal_task_queue: str,
    app_client: aiohttp.ClientSession,
    app_settings: dl_temporal.app.BaseTemporalWorkerAppSettings,
) -> None:
    handle = await temporal_client.start_workflow(
        workflows.Workflow,
        workflows.WorkflowParams.from_default(parent_context=dl_temporal.ParentContext(request_id="test_metrics")),
        id=str(uuid.uuid4()),
        task_queue=temporal_task_queue,
    )
    result = await handle.result()
    assert isinstance(result, workflows.WorkflowResult)

    assert await _scrape_metrics(app_client, app_settings, "test_workflow", "test_activity") == {
        Metric("temporal_workflow_execution_total", 'name="test_workflow",status="success"', 1.0),
        Metric("temporal_workflow_execution_duration_seconds_count", 'name="test_workflow",status="success"', 1.0),
        Metric("temporal_activity_execution_total", 'name="test_activity",status="success"', 1.0),
        Metric("temporal_activity_execution_duration_seconds_count", 'name="test_activity",status="success"', 1.0),
    }


@pytest.mark.asyncio
async def test_metrics_recorded_on_error_result(
    temporal_client: dl_temporal.TemporalClient,
    temporal_task_queue: str,
    app_client: aiohttp.ClientSession,
    app_settings: dl_temporal.app.BaseTemporalWorkerAppSettings,
) -> None:
    # return_error=True makes the activity return an ActivityError and the workflow a WorkflowError;
    # both surface as is_error results, so both metrics record status="error".
    handle = await temporal_client.start_workflow(
        workflows.Workflow,
        workflows.WorkflowParams.from_default(
            return_error=True,
            parent_context=dl_temporal.ParentContext(request_id="test_metrics"),
        ),
        id=str(uuid.uuid4()),
        task_queue=temporal_task_queue,
    )
    result = await handle.result()
    assert isinstance(result, workflows.WorkflowError)

    assert await _scrape_metrics(app_client, app_settings, "test_workflow", "test_activity") == {
        Metric("temporal_workflow_execution_total", 'name="test_workflow",status="error"', 1.0),
        Metric("temporal_workflow_execution_duration_seconds_count", 'name="test_workflow",status="error"', 1.0),
        Metric("temporal_activity_execution_total", 'name="test_activity",status="error"', 1.0),
        Metric("temporal_activity_execution_duration_seconds_count", 'name="test_activity",status="error"', 1.0),
    }


@pytest.mark.asyncio
async def test_metrics_recorded_on_failure(
    temporal_client: dl_temporal.TemporalClient,
    temporal_task_queue: str,
    app_client: aiohttp.ClientSession,
    app_settings: dl_temporal.app.BaseTemporalWorkerAppSettings,
) -> None:
    # The activity raises, which fails the workflow too, so both metrics record status="failure".
    handle = await temporal_client.start_workflow(
        workflows.RaisingWorkflow,
        workflows.RaisingWorkflow.Params(parent_context=dl_temporal.ParentContext(request_id="test_metrics")),
        id=str(uuid.uuid4()),
        task_queue=temporal_task_queue,
    )
    with pytest.raises(temporalio.client.WorkflowFailureError):
        await handle.result()

    assert await _scrape_metrics(app_client, app_settings, "test_raising_workflow", "test_raising_activity") == {
        Metric("temporal_workflow_execution_total", 'name="test_raising_workflow",status="failure"', 1.0),
        Metric(
            "temporal_workflow_execution_duration_seconds_count",
            'name="test_raising_workflow",status="failure"',
            1.0,
        ),
        Metric("temporal_activity_execution_total", 'name="test_raising_activity",status="failure"', 1.0),
        Metric(
            "temporal_activity_execution_duration_seconds_count",
            'name="test_raising_activity",status="failure"',
            1.0,
        ),
    }
