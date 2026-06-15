from collections.abc import Callable
import functools
from typing import Any
import weakref


def method_lru(maxsize: int = 128, typed: bool = False) -> Callable:
    """
    Hacked lru cache for methods
    """

    def wrapper(func: Callable) -> Callable:
        @functools.lru_cache(maxsize, typed)
        def _func(_self: Callable, *args: Any, **kwargs: Any) -> Any:
            return func(_self(), *args, **kwargs)

        @functools.wraps(func)
        def inner(self: object, *args: Any, **kwargs: Any) -> Callable:
            return _func(weakref.ref(self), *args, **kwargs)

        return inner

    return wrapper
