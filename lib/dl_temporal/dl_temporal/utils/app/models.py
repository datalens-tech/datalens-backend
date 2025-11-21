from typing import (
    Any,
    Coroutine,
)

import attr


@attr.define(frozen=True, kw_only=True)
class Callback:
    coroutine: Coroutine[Any, Any, None]
    name: str
    exception: bool = attr.field(default=True)
