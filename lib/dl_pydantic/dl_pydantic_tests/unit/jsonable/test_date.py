import datetime

import pydantic
import pytest

import dl_pydantic


JSON_VALUE = '"2025-01-02"'
DATE_VALUE = dl_pydantic.JsonableDate(2025, 1, 2)


def test_model_validate_with_original_type() -> None:
    class Model(dl_pydantic.BaseModel):
        value: dl_pydantic.JsonableDate

    original_value = datetime.date(2025, 1, 2)
    expected_value = DATE_VALUE

    model = Model.model_validate({"value": original_value})

    assert model.value == expected_value


def test_model_validate_json() -> None:
    class Model(dl_pydantic.BaseModel):
        value: dl_pydantic.JsonableDate

    model = Model.model_validate_json(f'{{"value": {JSON_VALUE}}}')

    assert model.value == DATE_VALUE


def test_raises_validation_error_on_none() -> None:
    class Model(dl_pydantic.BaseModel):
        value: dl_pydantic.JsonableDate

    with pytest.raises(pydantic.ValidationError):
        Model.model_validate({"value": None})


def test_model_dump_json() -> None:
    class Model(dl_pydantic.BaseModel):
        value: dl_pydantic.JsonableDate

    model = Model(value=DATE_VALUE)

    assert model.model_dump_json() == f'{{"value":{JSON_VALUE}}}'


def test_pydantic_core_schema() -> None:
    adapter = pydantic.TypeAdapter(dl_pydantic.JsonableDate)

    assert "json_schema" in adapter.core_schema
    assert "python_schema" in adapter.core_schema
