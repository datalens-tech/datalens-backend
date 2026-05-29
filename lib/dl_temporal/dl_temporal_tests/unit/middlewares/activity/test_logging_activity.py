import logging
from unittest import mock

import pytest

import dl_temporal


class _Params(dl_temporal.BaseActivityParams): ...


class _Result(dl_temporal.BaseActivityResult): ...


class _Error(dl_temporal.BaseActivityError): ...


@pytest.fixture(name="activity_info", autouse=True)
def fixture_activity_info(monkeypatch: pytest.MonkeyPatch) -> mock.Mock:
    info = mock.Mock()
    info.activity_type = "test_activity"
    info.activity_id = "act-1"
    info.attempt = 1
    info.workflow_id = "wf-1"
    info.workflow_run_id = "run-1"
    info.workflow_type = "test_workflow"
    info.workflow_namespace = "test_ns"
    info.task_queue = "test_queue"
    monkeypatch.setattr("temporalio.activity.info", lambda: info)
    return info


@pytest.fixture(name="activity")
def fixture_activity() -> mock.Mock:
    activity = mock.Mock(spec=dl_temporal.ActivityProtocol)
    activity.name = "test_activity"
    activity.logger = logging.getLogger("dl_temporal_tests.middlewares.activity.test_logging_activity")
    return activity


@pytest.mark.asyncio
async def test_success_logs_start_and_completed(
    activity: mock.Mock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG, logger=activity.logger.name)
    expected = _Result()

    async def handler(_: dl_temporal.BaseActivityParams) -> dl_temporal.BaseActivityResult:
        return expected

    result = await dl_temporal.LoggingActivityMiddleware().process(activity, _Params(), handler)

    assert result is expected
    messages = [record.getMessage() for record in caplog.records]
    assert any("starting with params" in m for m in messages)
    assert any("completed with result" in m for m in messages)
    assert not any("finished with error" in m for m in messages)


@pytest.mark.asyncio
async def test_error_result_logs_error(
    activity: mock.Mock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG, logger=activity.logger.name)
    expected = _Error()

    async def handler(_: dl_temporal.BaseActivityParams) -> dl_temporal.BaseActivityResult:
        return expected

    result = await dl_temporal.LoggingActivityMiddleware().process(activity, _Params(), handler)

    assert result is expected
    error_records = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert any("finished with error" in r.getMessage() for r in error_records)


@pytest.mark.asyncio
async def test_handler_exception_is_logged_and_re_raised(
    activity: mock.Mock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG, logger=activity.logger.name)

    async def handler(_: dl_temporal.BaseActivityParams) -> dl_temporal.BaseActivityResult:
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        await dl_temporal.LoggingActivityMiddleware().process(activity, _Params(), handler)

    error_records = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert any("failed" in r.getMessage() for r in error_records)
