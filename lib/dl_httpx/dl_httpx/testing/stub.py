import asyncio
from typing import (
    Any,
    Callable,
    Generic,
    TypeVar,
)

import attrs
from typing_extensions import Self


@attrs.define(kw_only=True)
class Call:
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    awaited: bool = attrs.field(default=False, init=False, eq=False)


CallableT = TypeVar("CallableT", bound=Callable)


@attrs.define(kw_only=True)
class TrackedCallable(Generic[CallableT]):
    method: CallableT

    calls: list[Call] = attrs.field(factory=list, init=False)
    # Cache of bound methods for each object instance if class method
    _bound_cache: dict[int, Any] = attrs.field(factory=dict, init=False)

    def __get__(self, obj: Any, objtype: Any = None) -> Self:
        if obj is None:
            return self
        obj_id = id(obj)
        if obj_id not in self._bound_cache:
            original = self.method
            bound: Any
            if asyncio.iscoroutinefunction(original):

                async def async_bound(*args: Any, **kwargs: Any) -> Any:
                    return await original(obj, *args, **kwargs)

                async_bound.__name__ = original.__name__
                bound = async_bound
            else:

                def sync_bound(*args: Any, **kwargs: Any) -> Any:
                    return original(obj, *args, **kwargs)

                sync_bound.__name__ = original.__name__
                bound = sync_bound
            self._bound_cache[obj_id] = TrackedCallable(method=bound)
        return self._bound_cache[obj_id]

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        call = Call(args=args, kwargs=kwargs)
        self.calls.append(call)

        result = self.method(*args, **kwargs)
        if asyncio.iscoroutine(result):

            async def _track_await(coro: Any, call: Call) -> Any:
                result = await coro
                call.awaited = True
                return result

            result = _track_await(result, call)

        return result

    def reset(self) -> None:
        self.calls.clear()

    def assert_called_once(self) -> None:
        assert (
            len(self.calls) == 1
        ), f"Expected {self.method.__name__!r} to be called once, but it was called {len(self.calls)} time(s)"

    def assert_called_once_with(self, *args: Any, **kwargs: Any) -> None:
        self.assert_called_once()
        call = self.calls[0]
        assert call.args == args and call.kwargs == kwargs, (
            f"Expected {self.method.__name__!r} to be called with args={args!r}, kwargs={kwargs!r}, "
            f"but was called with args={call.args!r}, kwargs={call.kwargs!r}"
        )

    def assert_not_called(self) -> None:
        assert (
            len(self.calls) == 0
        ), f"Expected {self.method.__name__!r} not to be called, but it was called {len(self.calls)} time(s)"

    def assert_awaited_once(self) -> None:
        awaited = [c for c in self.calls if c.awaited]
        assert len(awaited) > 0, f"Expected {self.method.__name__!r} to be awaited once, but it was not"

    def assert_awaited_once_with(self, *args: Any, **kwargs: Any) -> None:
        self.assert_awaited_once()
        call = self.calls[0]
        assert call.args == args and call.kwargs == kwargs, (
            f"Expected {self.method.__name__!r} to be awaited with args={args!r}, kwargs={kwargs!r}, "
            f"but was awaited with args={call.args!r}, kwargs={call.kwargs!r}"
        )

    def assert_not_awaited(self) -> None:
        awaited = [c for c in self.calls if c.awaited]
        assert (
            len(awaited) == 0
        ), f"Expected {self.method.__name__!r} not to be awaited, but it was awaited {len(awaited)} time(s)"


def tracked(func: CallableT) -> "TrackedCallable[CallableT]":
    return TrackedCallable(method=func)
