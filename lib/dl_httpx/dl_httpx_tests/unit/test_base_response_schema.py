import logging

import pydantic
import pytest

import dl_httpx


class _Schema(dl_httpx.BaseResponseSchema):
    value: int


def test_model_validate_success() -> None:
    result = _Schema.model_validate({"value": 1})

    assert result.value == 1


def test_model_validate_failure_dumps_raw_data(caplog: pytest.LogCaptureFixture) -> None:
    raw = {"value": "not-an-int", "extra": [1, 2, 3]}

    with caplog.at_level(logging.ERROR, logger="dl_httpx"), pytest.raises(pydantic.ValidationError):
        _Schema.model_validate(raw)

    matching = [record for record in caplog.records if "not-an-int" in record.getMessage()]
    assert matching, f"Expected raw data in log, got: {[r.getMessage() for r in caplog.records]}"
    assert _Schema.__name__ in matching[0].getMessage()


def test_model_validate_json_failure_dumps_raw_data(caplog: pytest.LogCaptureFixture) -> None:
    raw_json = '{"value": "not-an-int"}'

    with caplog.at_level(logging.ERROR, logger="dl_httpx"), pytest.raises(pydantic.ValidationError):
        _Schema.model_validate_json(raw_json)

    matching = [record for record in caplog.records if "not-an-int" in record.getMessage()]
    assert matching, f"Expected raw data in log, got: {[r.getMessage() for r in caplog.records]}"


def test_model_validate_failure_truncates_large_payload(caplog: pytest.LogCaptureFixture) -> None:
    raw = {"value": "not-an-int", "padding": "x" * 10_000}

    with caplog.at_level(logging.ERROR, logger="dl_httpx"), pytest.raises(pydantic.ValidationError):
        _Schema.model_validate(raw)

    assert len(caplog.records) == 1
    message = caplog.records[0].getMessage()
    assert "truncated" in message
    assert len(message) < 6000
