import asyncio
from typing import (
    Any,
    Callable,
    Coroutine,
)

import attr
from typing_extensions import Self


@attr.define(frozen=True, kw_only=True)
class Callback:
    coroutine: Coroutine[Any, Any, None]
    name: str
    exception: bool = attr.field(default=True)

    @classmethod
    def from_sync_function(
        cls,
        function: Callable[[], Any],
        name: str,
        exception: bool = True,
    ) -> Self:
        return cls(
            coroutine=asyncio.to_thread(function),
            name=name,
            exception=exception,
        )
