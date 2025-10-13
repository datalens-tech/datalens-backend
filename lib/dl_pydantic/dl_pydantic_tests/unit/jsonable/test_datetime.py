import datetime

import pydantic
import pytest

import dl_pydantic


def test_model_validate_with_original_type() -> None:
    class Model(dl_pydantic.BaseModel):
        value: dl_pydantic.JsonableDatetime

    original_value = datetime.datetime(2025, 1, 2, 3, 4, 5, 6)
    expected_value = dl_pydantic.JsonableDatetime(2025, 1, 2, 3, 4, 5, 6)

    model = Model.model_validate({"value": original_value})

    assert model.value == expected_value


def test_model_validate_json() -> None:
    class Model(dl_pydantic.BaseModel):
        value: dl_pydantic.JsonableDatetime

    model = Model.model_validate_json('{"value": "2025-01-02T03:04:05.000006Z"}')

    assert model.value == dl_pydantic.JsonableDatetime(2025, 1, 2, 3, 4, 5, 6, tzinfo=datetime.timezone.utc)


@pytest.mark.parametrize(
    "value,expected_json",
    [
        (
            dl_pydantic.JsonableDatetime(2025, 1, 2, 3, 4, 5),
            "2025-01-02T03:04:05.000000",
        ),
        (
            dl_pydantic.JsonableDatetime(2025, 1, 2, 3, 4, 5, 6),
            "2025-01-02T03:04:05.000006",
        ),
        (
            dl_pydantic.JsonableDatetime(2025, 1, 2, 3, 4, 5, 6, tzinfo=datetime.timezone.utc),
            "2025-01-02T03:04:05.000006Z",
        ),
        (
            dl_pydantic.JsonableDatetime(2025, 1, 2, 3, 4, 5, 6, tzinfo=datetime.timezone(datetime.timedelta(hours=3))),
            "2025-01-02T00:04:05.000006Z",
        ),
    ],
    ids=[
        "no_microseconds",
        "no_timezone",
        "timezone_utc",
        "timezone_offset",
    ],
)
def test_model_dump_json(value: dl_pydantic.JsonableDatetime, expected_json: str) -> None:
    class Model(dl_pydantic.BaseModel):
        value: dl_pydantic.JsonableDatetime

    model = Model(value=value)

    assert model.model_dump_json() == f'{{"value":"{expected_json}"}}'


def test_pydantic_core_schema() -> None:
    adapter = pydantic.TypeAdapter(dl_pydantic.JsonableDatetime)

    assert "json_schema" in adapter.core_schema
    assert "python_schema" in adapter.core_schema
