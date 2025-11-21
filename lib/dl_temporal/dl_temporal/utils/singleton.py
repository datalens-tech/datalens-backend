import functools
import inspect
import logging
from typing import (
    Any,
    Callable,
    Coroutine,
    TypeVar,
    cast,
)


LOGGER = logging.getLogger(__name__)


SINGLETON_FUNCTION_RESULT_ATTRIBUTE = "_singleton_function_result"
SyncFunction = Callable[..., Any]
AsyncFunction = Callable[..., Coroutine[Any, Any, Any]]
Function = SyncFunction | AsyncFunction

SyncFunctionType = TypeVar("SyncFunctionType", bound=SyncFunction)
AsyncFunctionType = TypeVar("AsyncFunctionType", bound=AsyncFunction)
FunctionType = TypeVar("FunctionType", bound=Function)


# Decorator is not thread-safe, but it's ok for our use case
def singleton_function_result(func: FunctionType) -> FunctionType:
    if inspect.iscoroutinefunction(func):
        return _async_singleton_function_result(func)
    else:
        return _sync_singleton_function_result(func)


def _async_singleton_function_result(func: AsyncFunctionType) -> AsyncFunctionType:
    @functools.wraps(func)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        if not hasattr(func, SINGLETON_FUNCTION_RESULT_ATTRIBUTE):
            setattr(func, SINGLETON_FUNCTION_RESULT_ATTRIBUTE, await func(*args, **kwargs))

        return getattr(func, SINGLETON_FUNCTION_RESULT_ATTRIBUTE)

    return cast(AsyncFunctionType, wrapper)


def _sync_singleton_function_result(func: SyncFunctionType) -> SyncFunctionType:
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        if not hasattr(func, SINGLETON_FUNCTION_RESULT_ATTRIBUTE):
            setattr(func, SINGLETON_FUNCTION_RESULT_ATTRIBUTE, func(*args, **kwargs))

        return getattr(func, SINGLETON_FUNCTION_RESULT_ATTRIBUTE)

    return cast(SyncFunctionType, wrapper)


# Decorator is not thread-safe, but it's ok for our use case
def singleton_class_method_result(func: FunctionType) -> FunctionType:
    if inspect.iscoroutinefunction(func):
        return _async_singleton_class_method_result(func)
    else:
        return _sync_singleton_class_method_result(func)


def _async_singleton_class_method_result(func: AsyncFunctionType) -> AsyncFunctionType:
    @functools.wraps(func)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        class_instance = args[0]
        instance_key = f"{SINGLETON_FUNCTION_RESULT_ATTRIBUTE}_{func.__name__}"

        if not hasattr(class_instance, instance_key):
            setattr(class_instance, instance_key, await func(*args, **kwargs))

        return getattr(class_instance, instance_key)

    return cast(AsyncFunctionType, wrapper)


def _sync_singleton_class_method_result(func: SyncFunctionType) -> SyncFunctionType:
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        class_instance = args[0]
        instance_key = f"{SINGLETON_FUNCTION_RESULT_ATTRIBUTE}_{func.__name__}"

        if not hasattr(class_instance, instance_key):
            setattr(class_instance, instance_key, func(*args, **kwargs))

        return getattr(class_instance, instance_key)

    return cast(SyncFunctionType, wrapper)
