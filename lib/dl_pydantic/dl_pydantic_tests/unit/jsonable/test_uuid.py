import uuid

import dl_pydantic


STRING_VALUE = "123e4567-e89b-12d3-a456-426614174000"
JSON_VALUE = f'"{STRING_VALUE}"'
ORIGINAL = uuid.UUID(STRING_VALUE)
EXPECTED = dl_pydantic.JsonableUUID(STRING_VALUE)


def test_model_validate_with_original_type() -> None:
    class Model(dl_pydantic.BaseModel):
        value: dl_pydantic.JsonableUUID

    model = Model.model_validate({"value": ORIGINAL})

    assert model.value == EXPECTED


def test_model_validate_json() -> None:
    class Model(dl_pydantic.BaseModel):
        value: dl_pydantic.JsonableUUID

    model = Model.model_validate_json(f'{{"value":{JSON_VALUE}}}')

    assert model.value == EXPECTED


def test_model_dump_json() -> None:
    class Model(dl_pydantic.BaseModel):
        value: dl_pydantic.JsonableUUID

    model = Model(value=EXPECTED)

    assert model.model_dump_json() == f'{{"value":{JSON_VALUE}}}'
