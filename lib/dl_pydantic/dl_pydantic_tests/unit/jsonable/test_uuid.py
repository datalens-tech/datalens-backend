import uuid

import pydantic
import pytest

import dl_pydantic


STRING_VALUE = "123e4567-e89b-12d3-a456-426614174000"
JSON_VALUE = '"123e4567-e89b-12d3-a456-426614174000"'
UUID_VALUE = dl_pydantic.JsonableUUID(STRING_VALUE)


def test_model_validate_with_original_type() -> None:
    class Model(dl_pydantic.BaseModel):
        value: dl_pydantic.JsonableUUID

    original_value = uuid.UUID(STRING_VALUE)
    expected_value = UUID_VALUE

    model = Model.model_validate({"value": original_value})

    assert model.value == expected_value


def test_model_validate_json() -> None:
    class Model(dl_pydantic.BaseModel):
        value: dl_pydantic.JsonableUUID

    model = Model.model_validate_json(f'{{"value":{JSON_VALUE}}}')

    assert model.value == UUID_VALUE


def test_raises_validation_error_on_none() -> None:
    class Model(dl_pydantic.BaseModel):
        value: dl_pydantic.JsonableUUID

    with pytest.raises(pydantic.ValidationError):
        Model.model_validate({"value": None})


def test_model_dump_json() -> None:
    class Model(dl_pydantic.BaseModel):
        value: dl_pydantic.JsonableUUID

    model = Model(value=UUID_VALUE)

    assert model.model_dump_json() == f'{{"value":{JSON_VALUE}}}'
