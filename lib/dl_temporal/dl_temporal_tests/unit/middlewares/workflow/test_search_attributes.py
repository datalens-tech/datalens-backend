from unittest import mock

import pytest
import temporalio.common

import dl_temporal


class _Params(dl_temporal.BaseWorkflowParams): ...


class _Result(dl_temporal.BaseWorkflowResult):
    @property
    def search_attributes(self) -> list[temporalio.common.SearchAttributeUpdate]:
        return [dl_temporal.SearchAttribute.RESULT_TYPE.keyword.value_set(dl_temporal.ResultType.SUCCESS)]


@pytest.fixture(name="upsert")
def fixture_upsert(monkeypatch: pytest.MonkeyPatch) -> mock.Mock:
    upsert = mock.Mock()
    monkeypatch.setattr("temporalio.workflow.upsert_search_attributes", upsert)
    return upsert


@pytest.mark.asyncio
async def test_upserts_result_search_attributes(upsert: mock.Mock) -> None:
    expected = _Result()

    async def handler(_: dl_temporal.BaseWorkflowParams) -> dl_temporal.BaseWorkflowResult:
        return expected

    result = await dl_temporal.SearchAttributesWorkflowMiddleware().process(mock.Mock(), _Params(), handler)

    assert result is expected
    upsert.assert_called_once_with(expected.search_attributes)


@pytest.mark.asyncio
async def test_empty_search_attributes_still_upserted(upsert: mock.Mock) -> None:
    # Default BaseWorkflowResult.search_attributes returns [] — middleware should
    # still call upsert (no-op on temporal's side) so behavior stays uniform.
    async def handler(_: dl_temporal.BaseWorkflowParams) -> dl_temporal.BaseWorkflowResult:
        return dl_temporal.BaseWorkflowResult()

    await dl_temporal.SearchAttributesWorkflowMiddleware().process(mock.Mock(), _Params(), handler)

    upsert.assert_called_once_with([])
