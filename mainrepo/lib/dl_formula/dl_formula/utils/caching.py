from __future__ import annotations

from functools import (
    _CacheInfo,
    lru_cache,
    wraps,
)
import threading
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Optional,
    Tuple,
    Type,
    TypeVar,
)


_FUNC = Callable[..., Any]
_QUALIFIER_VALUE_TV = TypeVar("_QUALIFIER_VALUE_TV")


class MultiCacheManager(Generic[_QUALIFIER_VALUE_TV]):
    def __init__(
        self,
        wrapped_function: _FUNC,
        maxsize: int,
        cache_exceptions: Tuple[Type[Exception], ...] = (),
        cache_qualifier: Optional[Callable[..., _QUALIFIER_VALUE_TV]] = None,
    ):
        self._wrapped_function = wrapped_function
        self._cache_exceptions = cache_exceptions
        self._maxsize = maxsize
        self._cache_qualifier = cache_qualifier
        self._cached_wrappers: Dict[Optional[_QUALIFIER_VALUE_TV], _FUNC] = {}
        self._cached_wrappers_lock = threading.Lock()

    def _get_qualifier_value(self, *args: Any, **kwargs: Any) -> Optional[_QUALIFIER_VALUE_TV]:
        if self._cache_qualifier is None:
            return None
        return self._cache_qualifier(*args, **kwargs)

    def _make_cached_wrapper(self) -> _FUNC:
        @lru_cache(self._maxsize)
        def _cached_result_and_error_wrapper(*args: Any, **kwargs: Any) -> Tuple[Optional[Exception], Any]:
            error: Optional[Exception] = None
            result: Any = None
            try:
                result = self._wrapped_function(*args, **kwargs)
            except self._cache_exceptions as err:
                error = err

            return error, result

        @wraps(self._wrapped_function)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            error, result = _cached_result_and_error_wrapper(*args, **kwargs)
            if error:
                raise error
            return result

        wrapper.cache_info = _cached_result_and_error_wrapper.cache_info  # type: ignore
        wrapper.cache_clear = _cached_result_and_error_wrapper.cache_clear  # type: ignore
        return wrapper

    def get_cached_wrapper_for_args(self, *args: Any, **kwargs: Any) -> _FUNC:
        qvalue = self._get_qualifier_value(*args, **kwargs)
        with self._cached_wrappers_lock:
            if qvalue not in self._cached_wrappers:
                wrapper = self._make_cached_wrapper()
                self._cached_wrappers[qvalue] = wrapper
            else:
                wrapper = self._cached_wrappers[qvalue]

        return wrapper

    def cache_info(self) -> Dict[Optional[_QUALIFIER_VALUE_TV], _CacheInfo]:
        """Collect cache info objects for all existing cache qualifier values."""
        with self._cached_wrappers_lock:
            return {qvalue: wrapper.cache_info() for qvalue, wrapper in self._cached_wrappers.items()}  # type: ignore

    def cache_clear(self) -> None:
        """Clear all existing caches."""
        with self._cached_wrappers_lock:
            for wrapper in self._cached_wrappers.values():
                wrapper.cache_clear()  # type: ignore


def multi_cached_with_errors(
    maxsize: int,
    cache_exceptions: Tuple[Type[Exception], ...] = (),
    cache_qualifier: Optional[Callable[..., _QUALIFIER_VALUE_TV]] = None,
) -> Callable[[_FUNC], _FUNC]:
    """
    Parameterized decorator that works just like ``lru_cache``,
    but instead of just caching results of successful executions,
    it also caches raised exceptions
    """

    def decorator(func: _FUNC) -> _FUNC:
        cache_manager: MultiCacheManager[_QUALIFIER_VALUE_TV] = MultiCacheManager(
            wrapped_function=func,
            maxsize=maxsize,
            cache_exceptions=cache_exceptions,
            cache_qualifier=cache_qualifier,
        )

        @wraps(func)
        def generic_wrapper(*args: Any, **kwargs: Any) -> Any:
            wrapper = cache_manager.get_cached_wrapper_for_args(*args, **kwargs)
            return wrapper(*args, **kwargs)

        generic_wrapper.multi_cache_manager = cache_manager  # type: ignore
        generic_wrapper.cache_clear = cache_manager.cache_clear  # type: ignore
        generic_wrapper.cache_info = cache_manager.cache_info  # type: ignore

        return generic_wrapper

    return decorator
