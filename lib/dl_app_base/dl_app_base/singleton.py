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

_LOCKED_AND_UNSET_VALUE = object()
_LOCKED_AND_UNSET_ERROR_MESSAGE_TEMPLATE = "Singleton result for {function_name} is locked and unset, but function is called again, probably because of a recursive call"


class LockedAndUnsetError(RuntimeError):
    ...


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
        function_name = func.__qualname__

        if not hasattr(func, SINGLETON_FUNCTION_RESULT_ATTRIBUTE):
            LOGGER.info("Creating %s singleton result", function_name)
            setattr(func, SINGLETON_FUNCTION_RESULT_ATTRIBUTE, _LOCKED_AND_UNSET_VALUE)
            result = await func(*args, **kwargs)
            setattr(func, SINGLETON_FUNCTION_RESULT_ATTRIBUTE, result)
            LOGGER.info("%s singleton result created", function_name)

        result = getattr(func, SINGLETON_FUNCTION_RESULT_ATTRIBUTE)
        if result is _LOCKED_AND_UNSET_VALUE:
            raise LockedAndUnsetError(_LOCKED_AND_UNSET_ERROR_MESSAGE_TEMPLATE.format(function_name=function_name))

        return result

    return cast(AsyncFunctionType, wrapper)


def _sync_singleton_function_result(func: SyncFunctionType) -> SyncFunctionType:
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        function_name = func.__qualname__

        if not hasattr(func, SINGLETON_FUNCTION_RESULT_ATTRIBUTE):
            LOGGER.info("Creating %s singleton result", function_name)
            setattr(func, SINGLETON_FUNCTION_RESULT_ATTRIBUTE, _LOCKED_AND_UNSET_VALUE)
            result = func(*args, **kwargs)
            setattr(func, SINGLETON_FUNCTION_RESULT_ATTRIBUTE, result)
            LOGGER.info("%s singleton result created", function_name)

        result = getattr(func, SINGLETON_FUNCTION_RESULT_ATTRIBUTE)
        if result is _LOCKED_AND_UNSET_VALUE:
            raise LockedAndUnsetError(_LOCKED_AND_UNSET_ERROR_MESSAGE_TEMPLATE.format(function_name=function_name))

        return result

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
        function_name = f"{func.__qualname__}"
        instance_key = f"{SINGLETON_FUNCTION_RESULT_ATTRIBUTE}.{function_name}"

        if not hasattr(class_instance, instance_key):
            LOGGER.info("Creating %s singleton result", function_name)
            setattr(class_instance, instance_key, _LOCKED_AND_UNSET_VALUE)
            result = await func(*args, **kwargs)
            setattr(class_instance, instance_key, result)
            LOGGER.info("%s singleton result created", function_name)

        result = getattr(class_instance, instance_key)
        if result is _LOCKED_AND_UNSET_VALUE:
            raise LockedAndUnsetError(_LOCKED_AND_UNSET_ERROR_MESSAGE_TEMPLATE.format(function_name=function_name))

        return result

    return cast(AsyncFunctionType, wrapper)


def _sync_singleton_class_method_result(func: SyncFunctionType) -> SyncFunctionType:
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        class_instance = args[0]
        function_name = f"{func.__qualname__}"
        instance_key = f"{SINGLETON_FUNCTION_RESULT_ATTRIBUTE}.{function_name}"

        if not hasattr(class_instance, instance_key):
            LOGGER.info("Creating %s singleton result", function_name)
            setattr(class_instance, instance_key, _LOCKED_AND_UNSET_VALUE)
            result = func(*args, **kwargs)
            setattr(class_instance, instance_key, result)
            LOGGER.info("%s singleton result created", function_name)

        result = getattr(class_instance, instance_key)
        if result is _LOCKED_AND_UNSET_VALUE:
            raise LockedAndUnsetError(_LOCKED_AND_UNSET_ERROR_MESSAGE_TEMPLATE.format(function_name=function_name))

        return result

    return cast(SyncFunctionType, wrapper)
