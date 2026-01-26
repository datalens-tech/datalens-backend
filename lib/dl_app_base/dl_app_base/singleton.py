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
            function_name = func.__name__
            LOGGER.info("Creating %s singleton result", function_name)
            result = await func(*args, **kwargs)
            setattr(func, SINGLETON_FUNCTION_RESULT_ATTRIBUTE, result)
            LOGGER.info("%s singleton result created", function_name)

        return getattr(func, SINGLETON_FUNCTION_RESULT_ATTRIBUTE)

    return cast(AsyncFunctionType, wrapper)


def _sync_singleton_function_result(func: SyncFunctionType) -> SyncFunctionType:
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        if not hasattr(func, SINGLETON_FUNCTION_RESULT_ATTRIBUTE):
            function_name = func.__name__
            LOGGER.info("Creating %s singleton result", function_name)
            result = func(*args, **kwargs)
            setattr(func, SINGLETON_FUNCTION_RESULT_ATTRIBUTE, result)
            LOGGER.info("%s singleton result created", function_name)

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
            function_name = f"{class_instance.__class__.__name__}.{func.__name__}"
            LOGGER.info("Creating %s singleton result", function_name)
            result = await func(*args, **kwargs)
            setattr(class_instance, instance_key, result)
            LOGGER.info("%s singleton result created", function_name)

        return getattr(class_instance, instance_key)

    return cast(AsyncFunctionType, wrapper)


def _sync_singleton_class_method_result(func: SyncFunctionType) -> SyncFunctionType:
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        class_instance = args[0]
        instance_key = f"{SINGLETON_FUNCTION_RESULT_ATTRIBUTE}_{func.__name__}"

        if not hasattr(class_instance, instance_key):
            function_name = f"{class_instance.__class__.__name__}.{func.__name__}"
            LOGGER.info("Creating %s singleton result", function_name)
            result = func(*args, **kwargs)
            setattr(class_instance, instance_key, result)
            LOGGER.info("%s singleton result created", function_name)

        return getattr(class_instance, instance_key)

    return cast(SyncFunctionType, wrapper)
