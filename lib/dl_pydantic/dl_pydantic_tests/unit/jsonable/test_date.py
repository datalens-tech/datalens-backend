import datetime

import pydantic
import pytest

import dl_pydantic


JSON_VALUE = '"2025-01-02"'
ORIGINAL = datetime.date(2025, 1, 2)
EXPECTED = dl_pydantic.JsonableDate(2025, 1, 2)


def test_from_original() -> None:
    assert dl_pydantic.JsonableDate.from_original(ORIGINAL) == EXPECTED


def test_model_validate_with_original_type() -> None:
    class Model(dl_pydantic.BaseModel):
        value: dl_pydantic.JsonableDate

    model = Model.model_validate({"value": ORIGINAL})

    assert model.value == EXPECTED


def test_model_validate_json() -> None:
    class Model(dl_pydantic.BaseModel):
        value: dl_pydantic.JsonableDate

    model = Model.model_validate_json(f'{{"value": {JSON_VALUE}}}')

    assert model.value == EXPECTED


def test_raises_validation_error_on_none() -> None:
    class Model(dl_pydantic.BaseModel):
        value: dl_pydantic.JsonableDate

    with pytest.raises(pydantic.ValidationError):
        Model.model_validate({"value": None})


def test_model_dump_json() -> None:
    class Model(dl_pydantic.BaseModel):
        value: dl_pydantic.JsonableDate

    model = Model(value=EXPECTED)

    assert model.model_dump_json() == f'{{"value":{JSON_VALUE}}}'


def test_pydantic_core_schema() -> None:
    adapter = pydantic.TypeAdapter(dl_pydantic.JsonableDate)

    assert "json_schema" in adapter.core_schema
    assert "python_schema" in adapter.core_schema
