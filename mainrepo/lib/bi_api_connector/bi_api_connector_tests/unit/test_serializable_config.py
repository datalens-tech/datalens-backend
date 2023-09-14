from enum import Enum
from typing import Optional

import attr

from bi_api_connector.form_config.models.common import (
    SerializableConfig,
    inner,
    remap,
    skip_if_null,
)


class MyEnum(Enum):
    one = "one"


@attr.s(kw_only=True, frozen=True)
class SomeItemConfig(SerializableConfig):
    my_field: MyEnum = attr.ib()


@attr.s(kw_only=True, frozen=True)
class SomeConfig(SerializableConfig):
    basic_field: str = attr.ib()
    enum_field: MyEnum = attr.ib()
    undefined: Optional[int] = attr.ib(default=None, metadata=skip_if_null())
    remapped: str = attr.ib(default="orig_value", metadata=remap("remapped_2"))
    list_field: list[SomeItemConfig] = attr.ib()
    dict_field: dict[MyEnum, list[SomeItemConfig]] = attr.ib()
    magic_field: int = attr.ib(default=637, metadata=inner())


def test_config_as_dict():
    my_config = SomeConfig(
        basic_field="asdf",
        enum_field=MyEnum.one,
        list_field=[
            SomeItemConfig(my_field=MyEnum.one),
        ],
        dict_field={
            MyEnum.one: [
                SomeItemConfig(my_field=MyEnum.one),
                SomeItemConfig(my_field=MyEnum.one),
            ]
        },
    )

    result = my_config.as_dict()
    assert "undefined" not in result
    assert "magic_field" not in result
    assert result == {
        "basic_field": "asdf",
        "enum_field": "one",
        "remapped_2": "orig_value",
        "list_field": [
            {"my_field": "one"},
        ],
        "dict_field": {
            "one": [
                {"my_field": "one"},
                {"my_field": "one"},
            ],
        },
    }
