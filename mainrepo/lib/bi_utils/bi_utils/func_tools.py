import functools
import weakref
from typing import Callable, Any


def method_lru(maxsize: int = 128, typed: bool = False) -> Callable:
    """
    Hacked lru cache for methods
    """
    def wrapper(func: Callable) -> Callable:
        @functools.lru_cache(maxsize, typed)
        def _func(_self: Callable, *args, **kwargs) -> Any:  # type: ignore
            return func(_self(), *args, **kwargs)

        @functools.wraps(func)
        def inner(self: object, *args, **kwargs) -> Callable:  # type: ignore
            return _func(weakref.ref(self), *args, **kwargs)

        return inner

    return wrapper
