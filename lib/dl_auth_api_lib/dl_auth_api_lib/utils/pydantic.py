from typing import (
    Any,
    Callable,
    Hashable,
    TypeVar,
)


T = TypeVar("T")


def make_dict_factory(factory: Callable[[Any], T]) -> Callable[[Any], dict[Hashable, T]]:
    def dict_factory(v: Any) -> dict[Hashable, T]:
        assert isinstance(v, dict)
        return {name: factory(item) for name, item in v.items()}

    return dict_factory
