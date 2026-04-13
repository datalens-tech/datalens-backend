from typing import (
    Awaitable,
    Callable,
)

import dl_temporal
import dl_utils


class NestedModel(dl_temporal.BaseModel):
    test_int: int


async def await_for_success(
    name: str,
    condition: Callable[[], Awaitable[None]],
) -> tuple[bool, str]:
    async def _wrapped() -> bool:
        try:
            await condition()
        except Exception:
            return False
        return True

    return await dl_utils.await_for(name, _wrapped, timeout=1, interval=0.1)
