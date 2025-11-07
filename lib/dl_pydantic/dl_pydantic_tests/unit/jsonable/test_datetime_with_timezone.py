import datetime

import pydantic
import pytest

import dl_pydantic


ORIGINAL = datetime.datetime(2025, 1, 2, 3, 4, 5, 6, tzinfo=datetime.timezone.utc)
EXPECTED = dl_pydantic.JsonableDatetimeWithTimeZone(2025, 1, 2, 3, 4, 5, 6, tzinfo=datetime.timezone.utc)


def test_from_original() -> None:
    assert dl_pydantic.JsonableDatetimeWithTimeZone.from_original(ORIGINAL) == EXPECTED


def test_model_validate_with_original_type() -> None:
    class Model(dl_pydantic.BaseModel):
        value: dl_pydantic.JsonableDatetimeWithTimeZone

    model = Model.model_validate({"value": ORIGINAL})

    assert model.value == EXPECTED


@pytest.mark.parametrize(
    "value,expected_value",
    [
        (
            "2025-01-02T03:04:05.000006Z",
            dl_pydantic.JsonableDatetimeWithTimeZone(2025, 1, 2, 3, 4, 5, 6, tzinfo=datetime.timezone.utc),
        ),
        (
            "2025-01-02T03:04:05.000006+03:00",
            dl_pydantic.JsonableDatetimeWithTimeZone(
                2025, 1, 2, 3, 4, 5, 6, tzinfo=datetime.timezone(datetime.timedelta(hours=3))
            ),
        ),
        (
            "2025-01-02T03:04:05Z",
            dl_pydantic.JsonableDatetimeWithTimeZone(2025, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc),
        ),
        (
            "2025-01-02T03:04:05.006Z",
            dl_pydantic.JsonableDatetimeWithTimeZone(2025, 1, 2, 3, 4, 5, 6000, tzinfo=datetime.timezone.utc),
        ),
    ],
    ids=[
        "timezone_utc",
        "timezone_offset",
        "no_microseconds",
        "microseconds_different_precision",
    ],
)
def test_model_validate_json(
    value: str,
    expected_value: dl_pydantic.JsonableDatetimeWithTimeZone,
) -> None:
    class Model(dl_pydantic.BaseModel):
        value: dl_pydantic.JsonableDatetimeWithTimeZone

    model = Model.model_validate_json(f'{{"value": "{value}"}}')

    assert model.value == expected_value


def test_raises_validation_error_on_none() -> None:
    class Model(dl_pydantic.BaseModel):
        value: dl_pydantic.JsonableDatetimeWithTimeZone

    with pytest.raises(pydantic.ValidationError):
        Model.model_validate({"value": None})


@pytest.mark.parametrize(
    "value,expected_json",
    [
        (
            dl_pydantic.JsonableDatetimeWithTimeZone(2025, 1, 2, 3, 4, 5, 6, tzinfo=datetime.timezone.utc),
            "2025-01-02T03:04:05.000006Z",
        ),
        (
            dl_pydantic.JsonableDatetimeWithTimeZone(
                2025, 1, 2, 3, 4, 5, 6, tzinfo=datetime.timezone(datetime.timedelta(hours=3))
            ),
            "2025-01-02T00:04:05.000006Z",
        ),
        (
            dl_pydantic.JsonableDatetimeWithTimeZone(2025, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc),
            "2025-01-02T03:04:05.000000Z",
        ),
    ],
    ids=[
        "timezone_utc",
        "timezone_offset",
        "no_microseconds",
    ],
)
def test_model_dump_json(value: dl_pydantic.JsonableDatetimeWithTimeZone, expected_json: str) -> None:
    class Model(dl_pydantic.BaseModel):
        value: dl_pydantic.JsonableDatetimeWithTimeZone

    model = Model(value=value)

    assert model.model_dump_json() == f'{{"value":"{expected_json}"}}'


def test_pydantic_core_schema() -> None:
    adapter = pydantic.TypeAdapter(dl_pydantic.JsonableDatetimeWithTimeZone)

    assert "json_schema" in adapter.core_schema
    assert "python_schema" in adapter.core_schema


def test_raises_value_error_if_tzinfo_is_none() -> None:
    with pytest.raises(ValueError):
        dl_pydantic.JsonableDatetimeWithTimeZone.from_original(datetime.datetime(2025, 1, 2, 3, 4, 5, 6))

    with pytest.raises(ValueError):
        dl_pydantic.JsonableDatetimeWithTimeZone(2025, 1, 2, 3, 4, 5, 6)

    with pytest.raises(ValueError):
        dl_pydantic.JsonableDatetimeWithTimeZone(2025, 1, 2, 3, 4, 5, 6, tzinfo=None)

    class Model(dl_pydantic.BaseModel):
        value: dl_pydantic.JsonableDatetimeWithTimeZone

    with pytest.raises(ValueError):
        Model.model_validate_json('{{"value": "2025-01-02T03:04:05.000006"}}')
