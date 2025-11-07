import datetime

import dl_pydantic


JSON_VALUE = '"P9DT3H4M5.006007S"'
ORIGINAL = datetime.timedelta(weeks=1, days=2, hours=3, minutes=4, seconds=5, milliseconds=6, microseconds=7)
EXPECTED = dl_pydantic.JsonableTimedelta(weeks=1, days=2, hours=3, minutes=4, seconds=5, milliseconds=6, microseconds=7)


def test_model_validate_with_original_type() -> None:
    class Model(dl_pydantic.BaseModel):
        value: dl_pydantic.JsonableTimedelta

    model = Model.model_validate({"value": ORIGINAL})

    assert model.value == EXPECTED


def test_model_validate_json() -> None:
    class Model(dl_pydantic.BaseModel):
        value: dl_pydantic.JsonableTimedelta

    model = Model.model_validate_json(f'{{"value": {JSON_VALUE}}}')

    assert model.value == EXPECTED


def test_model_dump_json() -> None:
    class Model(dl_pydantic.BaseModel):
        value: dl_pydantic.JsonableTimedelta

    model = Model(value=EXPECTED)

    assert model.model_dump_json() == f'{{"value":{JSON_VALUE}}}'
