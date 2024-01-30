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
        def _func(_self: Callable, *args, **kwargs) -> Any:
            return func(_self(), *args, **kwargs)

        @functools.wraps(func)
        def inner(self: object, *args, **kwargs) -> Callable:
            return _func(weakref.ref(self), *args, **kwargs)

        return inner

    return wrapper
