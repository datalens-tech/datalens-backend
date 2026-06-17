import datetime
import json
from typing import Annotated

import pydantic
import pytest

import dl_temporal


class _Plain(dl_temporal.BaseModel):
    visible_one: str
    visible_two: int


class _WithExcluded(dl_temporal.BaseModel):
    visible: str
    secret: Annotated[str, dl_temporal.LoggingExcluded]
    payload: Annotated[dict[str, str], dl_temporal.LoggingExcluded]


class _Inner(dl_temporal.BaseModel):
    secret: Annotated[str, dl_temporal.LoggingExcluded]
    visible: str


class _Outer(dl_temporal.BaseModel):
    name: str
    inner: _Inner


def test_model_dump_for_logging_no_annotations() -> None:
    model = _Plain(visible_one="foo", visible_two=42)
    result = json.loads(model.model_dump_for_logging())
    assert result == {"visible_one": "foo", "visible_two": 42}


def test_model_dump_for_logging_excludes_annotated_fields() -> None:
    model = _WithExcluded(visible="ok", secret="hide-me", payload={"k": "v"})
    result = json.loads(model.model_dump_for_logging())
    assert result == {"visible": "ok"}


def test_model_dump_for_logging_exclusion_does_not_recurse() -> None:
    model = _Outer(name="outer", inner=_Inner(secret="hide-me", visible="show-me"))
    result = json.loads(model.model_dump_for_logging())
    assert result == {"name": "outer", "inner": {"secret": "hide-me", "visible": "show-me"}}


def test_activity_params_reject_close_smaller_than_start_timeout() -> None:
    with pytest.raises(pydantic.ValidationError):
        dl_temporal.BaseActivityParams(
            start_to_close_timeout=datetime.timedelta(minutes=5),
            schedule_to_close_timeout=datetime.timedelta(minutes=1),
        )
