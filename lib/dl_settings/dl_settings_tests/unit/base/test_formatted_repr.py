import enum
from typing import (
    Any,
    Optional,
)

import frozendict
import pydantic

import dl_settings


class _Color(enum.Enum):
    RED = 1
    BLUE = 2


def test_str() -> None:
    class Settings(dl_settings.BaseRootSettings):
        name: str = "hello"

    assert Settings().model_formatted_repr() == "Settings:\n  name: 'hello'"


def test_int() -> None:
    class Settings(dl_settings.BaseRootSettings):
        count: int = 42

    assert Settings().model_formatted_repr() == "Settings:\n  count: 42"


def test_float() -> None:
    class Settings(dl_settings.BaseRootSettings):
        rate: float = 3.14

    assert Settings().model_formatted_repr() == "Settings:\n  rate: 3.14"


def test_bool() -> None:
    class Settings(dl_settings.BaseRootSettings):
        enabled: bool = True

    assert Settings().model_formatted_repr() == "Settings:\n  enabled: True"


def test_none() -> None:
    class Settings(dl_settings.BaseRootSettings):
        value: Optional[str] = None

    assert Settings().model_formatted_repr() == "Settings:\n  value: None"


def test_enum() -> None:
    class Settings(dl_settings.BaseRootSettings):
        color: _Color = _Color.RED

    assert Settings().model_formatted_repr() == "Settings:\n  color: <_Color.RED: 1>"


def test_custom_class() -> None:
    class MyClass:
        def __repr__(self) -> str:
            return "MyClass()"

    class Settings(dl_settings.BaseRootSettings):
        obj: Any = pydantic.Field(default_factory=MyClass)

    assert Settings().model_formatted_repr() == "Settings:\n  obj: MyClass()"


def test_repr_false_excluded() -> None:
    class Settings(dl_settings.BaseRootSettings):
        name: str = "hello"
        password: str = pydantic.Field(default="secret", repr=False)

    assert Settings().model_formatted_repr() == "Settings:\n  name: 'hello'"


def test_nested_model() -> None:
    class DB(dl_settings.BaseSettings):
        host: str = "localhost"

    class Settings(dl_settings.BaseRootSettings):
        db: DB = pydantic.Field(default_factory=DB)

    assert Settings().model_formatted_repr() == "Settings:\n  db: DB:\n    host: 'localhost'"


def test_nested_model_repr_false() -> None:
    class DB(dl_settings.BaseSettings):
        host: str = "localhost"
        password: str = pydantic.Field(default="secret", repr=False)

    class Settings(dl_settings.BaseRootSettings):
        db: DB = pydantic.Field(default_factory=DB)

    assert Settings().model_formatted_repr() == "Settings:\n  db: DB:\n    host: 'localhost'"


def test_list() -> None:
    class Settings(dl_settings.BaseRootSettings):
        items: list[str] = pydantic.Field(default_factory=lambda: ["a", "b"])

    assert Settings().model_formatted_repr() == "Settings:\n  items: \n    - 'a'\n    - 'b'"


def test_empty_list() -> None:
    class Settings(dl_settings.BaseRootSettings):
        items: list[str] = pydantic.Field(default_factory=list)

    assert Settings().model_formatted_repr() == "Settings:\n  items: []"


def test_tuple() -> None:
    class Settings(dl_settings.BaseRootSettings):
        items: tuple[str, ...] = pydantic.Field(default_factory=lambda: ("a", "b"))

    assert Settings().model_formatted_repr() == "Settings:\n  items: \n    - 'a'\n    - 'b'"


def test_list_of_nested_models() -> None:
    class Item(dl_settings.BaseSettings):
        value: int
        secret: str = pydantic.Field(default="hidden", repr=False)

    class Settings(dl_settings.BaseRootSettings):
        items: list[Item] = pydantic.Field(default_factory=lambda: [Item(value=1), Item(value=2)])

    assert Settings().model_formatted_repr() == (
        "Settings:\n" "  items: \n" "    - Item:\n" "      value: 1\n" "    - Item:\n" "      value: 2"
    )


def test_tuple_of_nested_models() -> None:
    class Item(dl_settings.BaseSettings):
        value: int
        secret: str = pydantic.Field(default="hidden", repr=False)

    class Settings(dl_settings.BaseRootSettings):
        items: tuple[Item, ...] = pydantic.Field(default_factory=lambda: (Item(value=1), Item(value=2)))

    assert Settings().model_formatted_repr() == (
        "Settings:\n" "  items: \n" "    - Item:\n" "      value: 1\n" "    - Item:\n" "      value: 2"
    )


def test_dict_of_nested_models() -> None:
    class Item(dl_settings.BaseSettings):
        value: int
        secret: str = pydantic.Field(default="hidden", repr=False)

    class Settings(dl_settings.BaseRootSettings):
        mapping: dict[str, Item] = pydantic.Field(default_factory=lambda: {"a": Item(value=1), "b": Item(value=2)})

    assert Settings().model_formatted_repr() == (
        "Settings:\n" "  mapping: \n" "    a: Item:\n" "      value: 1\n" "    b: Item:\n" "      value: 2"
    )


def test_deeply_nested_repr_false() -> None:
    class Inner(dl_settings.BaseSettings):
        value: int
        secret: str = pydantic.Field(default="hidden", repr=False)

    class Outer(dl_settings.BaseSettings):
        items: list[Inner] = pydantic.Field(default_factory=lambda: [Inner(value=1)])
        password: str = pydantic.Field(default="pass", repr=False)

    class Settings(dl_settings.BaseRootSettings):
        outer: Outer = pydantic.Field(default_factory=Outer)

    assert Settings().model_formatted_repr() == (
        "Settings:\n" "  outer: Outer:\n" "    items: \n" "      - Inner:\n" "        value: 1"
    )


def test_dict() -> None:
    class Settings(dl_settings.BaseRootSettings):
        mapping: dict[str, int] = pydantic.Field(default_factory=lambda: {"x": 1, "y": 2})

    assert Settings().model_formatted_repr() == "Settings:\n  mapping: \n    x: 1\n    y: 2"


def test_empty_dict() -> None:
    class Settings(dl_settings.BaseRootSettings):
        mapping: dict[str, int] = pydantic.Field(default_factory=dict)

    assert Settings().model_formatted_repr() == "Settings:\n  mapping: {}"


def test_set() -> None:
    class Settings(dl_settings.BaseRootSettings):
        values: set[int] = pydantic.Field(default_factory=lambda: {1, 2, 3})

    assert Settings().model_formatted_repr() == "Settings:\n  values: \n    - 1\n    - 2\n    - 3"


def test_empty_set() -> None:
    class Settings(dl_settings.BaseRootSettings):
        values: set[int] = pydantic.Field(default_factory=set)

    assert Settings().model_formatted_repr() == "Settings:\n  values: set()"


def test_frozenset() -> None:
    class Settings(dl_settings.BaseRootSettings):
        values: frozenset[int] = pydantic.Field(default_factory=lambda: frozenset({1, 2, 3}))

    assert Settings().model_formatted_repr() == "Settings:\n  values: \n    - 1\n    - 2\n    - 3"


def test_empty_frozenset() -> None:
    class Settings(dl_settings.BaseRootSettings):
        values: frozenset[int] = pydantic.Field(default_factory=frozenset)

    assert Settings().model_formatted_repr() == "Settings:\n  values: frozenset()"


def test_frozendict() -> None:
    class Settings(dl_settings.BaseRootSettings):
        mapping: frozendict.frozendict[str, int] = pydantic.Field(
            default_factory=lambda: frozendict.frozendict({"x": 1, "y": 2})
        )

    assert Settings().model_formatted_repr() == "Settings:\n  mapping: \n    x: 1\n    y: 2"


def test_empty_frozendict() -> None:
    class Settings(dl_settings.BaseRootSettings):
        mapping: frozendict.frozendict[str, int] = pydantic.Field(default_factory=frozendict.frozendict)

    assert Settings().model_formatted_repr() == "Settings:\n  mapping: {}"
