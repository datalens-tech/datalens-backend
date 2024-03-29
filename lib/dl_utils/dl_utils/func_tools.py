import functools
from typing import (
    Any,
    Callable,
)
import weakref


def method_lru(maxsize: int = 128, typed: bool = False) -> Callable:
    """
    Hacked lru cache for methods
    """

    def wrapper(func: Callable) -> Callable:
        @functools.lru_cache(maxsize, typed)
        def _func(_self: Callable, *args, **kwargs) -> Any:  # type: ignore  # 2024-01-30 # TODO: Function is missing a type annotation for one or more arguments  [no-untyped-def]
            return func(_self(), *args, **kwargs)

        @functools.wraps(func)
        def inner(self: object, *args, **kwargs) -> Callable:  # type: ignore  # 2024-01-30 # TODO: Function is missing a type annotation for one or more arguments  [no-untyped-def]
            return _func(weakref.ref(self), *args, **kwargs)

        return inner

    return wrapper
