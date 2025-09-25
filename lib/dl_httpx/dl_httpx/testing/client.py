import asyncio
from typing import (
    Any,
    Awaitable,
    Callable,
)

import attrs


@attrs.define(kw_only=True, auto_attribs=True, frozen=True)
class TestingHttpxClient:
    sync_client: Any
    async_client: Any

    def _sync_method_wrapper(
        self,
        sync_client_method: Callable[..., Any],
        async_client_method: Callable[..., Any],
    ) -> Callable[..., Any]:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            sync_response = sync_client_method(*args, **kwargs)
            async_response = async_client_method(*args, **kwargs)

            assert sync_response == async_response

            return sync_response

        return wrapper

    def _async_method_wrapper(
        self,
        sync_client_method: Callable[..., Any],
        async_client_method: Callable[..., Awaitable[Any]],
    ) -> Callable[..., Awaitable[Any]]:
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            async_response = await async_client_method(*args, **kwargs)
            sync_response = sync_client_method(*args, **kwargs)

            assert async_response == sync_response

            return async_response

        return wrapper

    def __getattr__(self, name: str) -> Callable[..., Any]:
        sync_client_method = getattr(self.sync_client, name)
        async_client_method = getattr(self.async_client, name)

        if asyncio.iscoroutinefunction(async_client_method):
            return self._async_method_wrapper(sync_client_method, async_client_method)

        return self._sync_method_wrapper(sync_client_method, async_client_method)


__all__ = [
    "TestingHttpxClient",
]
