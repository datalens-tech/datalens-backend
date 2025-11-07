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


@pytest.mark.parametrize(
    "value,expected_value",
    [
        (
            "2025-01-02T03:04:05.000006Z",
            dl_pydantic.JsonableDatetime(2025, 1, 2, 3, 4, 5, 6, tzinfo=datetime.timezone.utc),
        ),
        (
            "2025-01-02T03:04:05.000006+03:00",
            dl_pydantic.JsonableDatetime(2025, 1, 2, 3, 4, 5, 6, tzinfo=datetime.timezone(datetime.timedelta(hours=3))),
        ),
        (
            "2025-01-02T03:04:05Z",
            dl_pydantic.JsonableDatetime(2025, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc),
        ),
        (
            "2025-01-02T03:04:05.006Z",
            dl_pydantic.JsonableDatetime(2025, 1, 2, 3, 4, 5, 6000, tzinfo=datetime.timezone.utc),
        ),
    ],
    ids=[
        "utc",
        "offset",
        "no_microseconds",
        "microseconds_different_precision",
    ],
)
def test_model_validate_json(
    value: str,
    expected_value: dl_pydantic.JsonableDatetime,
) -> None:
    class Model(dl_pydantic.BaseModel):
        value: dl_pydantic.JsonableDatetime

    model = Model.model_validate_json(f'{{"value": "{value}"}}')

    assert model.value == expected_value


def test_raises_validation_error_on_none() -> None:
    class Model(dl_pydantic.BaseModel):
        value: dl_pydantic.JsonableDatetime

    with pytest.raises(pydantic.ValidationError):
        Model.model_validate({"value": None})


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
