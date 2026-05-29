from unittest import mock

import pytest

import dl_temporal


class _Params(dl_temporal.BaseWorkflowParams): ...


class _Result(dl_temporal.BaseWorkflowResult): ...


@pytest.fixture(name="workflow_info", autouse=True)
def fixture_workflow_info(monkeypatch: pytest.MonkeyPatch) -> mock.Mock:
    info = mock.Mock()
    info.run_id = "run-123"
    monkeypatch.setattr("temporalio.workflow.info", lambda: info)
    return info


@pytest.fixture(name="workflow")
def fixture_workflow() -> mock.Mock:
    return mock.Mock()


@pytest.mark.asyncio
async def test_sets_request_id_when_missing(
    workflow: mock.Mock,
    workflow_info: mock.Mock,
) -> None:
    params = _Params()
    assert params.parent_context.request_id is None

    seen: dict[str, str | None] = {}

    async def handler(p: dl_temporal.BaseWorkflowParams) -> dl_temporal.BaseWorkflowResult:
        seen["request_id"] = p.parent_context.request_id
        return _Result()

    await dl_temporal.ParentContextWorkflowMiddleware().process(workflow, params, handler)

    assert seen["request_id"] == workflow_info.run_id
    assert params.parent_context.request_id == workflow_info.run_id


@pytest.mark.asyncio
async def test_preserves_existing_request_id(
    workflow: mock.Mock,
) -> None:
    params = _Params(parent_context=dl_temporal.ParentContext(request_id="caller-set"))

    async def handler(p: dl_temporal.BaseWorkflowParams) -> dl_temporal.BaseWorkflowResult:
        assert p.parent_context.request_id == "caller-set"
        return _Result()

    await dl_temporal.ParentContextWorkflowMiddleware().process(workflow, params, handler)

    assert params.parent_context.request_id == "caller-set"
