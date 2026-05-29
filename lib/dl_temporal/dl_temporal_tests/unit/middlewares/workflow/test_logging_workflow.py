import logging
from unittest import mock

import pytest

import dl_temporal


class _Params(dl_temporal.BaseWorkflowParams): ...


class _Result(dl_temporal.BaseWorkflowResult): ...


class _Error(dl_temporal.BaseWorkflowError): ...


@pytest.fixture(name="workflow_info", autouse=True)
def fixture_workflow_info(monkeypatch: pytest.MonkeyPatch) -> mock.Mock:
    info = mock.Mock()
    info.workflow_type = "test_workflow"
    info.workflow_id = "wf-1"
    info.run_id = "run-1"
    info.namespace = "test_ns"
    info.task_queue = "test_queue"
    monkeypatch.setattr("temporalio.workflow.info", lambda: info)
    return info


@pytest.fixture(name="workflow")
def fixture_workflow() -> mock.Mock:
    workflow = mock.Mock()
    workflow.name = "test_workflow"
    workflow.logger = logging.getLogger("dl_temporal_tests.middlewares.workflow.test_logging_workflow")
    return workflow


@pytest.mark.asyncio
async def test_success_logs_start_and_completed(
    workflow: mock.Mock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG, logger=workflow.logger.name)
    expected = _Result()

    async def handler(_: dl_temporal.BaseWorkflowParams) -> dl_temporal.BaseWorkflowResult:
        return expected

    result = await dl_temporal.LoggingWorkflowMiddleware().process(workflow, _Params(), handler)

    assert result is expected
    messages = [record.getMessage() for record in caplog.records]
    assert any("starting with params" in m for m in messages)
    assert any("completed with result" in m for m in messages)
    assert not any("finished with error" in m for m in messages)


@pytest.mark.asyncio
async def test_error_result_logs_error(
    workflow: mock.Mock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG, logger=workflow.logger.name)
    expected = _Error()

    async def handler(_: dl_temporal.BaseWorkflowParams) -> dl_temporal.BaseWorkflowResult:
        return expected

    result = await dl_temporal.LoggingWorkflowMiddleware().process(workflow, _Params(), handler)

    assert result is expected
    error_records = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert any("finished with error" in r.getMessage() for r in error_records)


@pytest.mark.asyncio
async def test_handler_exception_is_logged_and_re_raised(
    workflow: mock.Mock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG, logger=workflow.logger.name)

    async def handler(_: dl_temporal.BaseWorkflowParams) -> dl_temporal.BaseWorkflowResult:
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        await dl_temporal.LoggingWorkflowMiddleware().process(workflow, _Params(), handler)

    error_records = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert any("failed" in r.getMessage() for r in error_records)
