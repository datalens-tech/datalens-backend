from __future__ import annotations

import sys
from typing import (
    TYPE_CHECKING,
    AsyncContextManager,
    Awaitable,
    Callable,
    Optional,
    Tuple,
    TypeVar,
)

if sys.version_info >= (3, 8):
    from typing import Protocol
else:
    from typing_extensions import Protocol

if TYPE_CHECKING:
    from redis.asyncio import Redis


# `generate_func` types:
# `generate_func` must return a bytestring for saving to redis,
# but can also return a value to be returned directly,
# to skip an unnecessary deserialization (if there was no cache hit).
# The cache wrap can return either the full result, or only the bytestring part.
_GF_RET_TV = TypeVar("_GF_RET_TV")
TGenerateResult = Tuple[bytes, _GF_RET_TV]
TCacheResult = Tuple[bytes, Optional[_GF_RET_TV]]
TGenerateFunc = Callable[[], Awaitable[TGenerateResult]]


class TClientACM(Protocol):
    """
    Redis Client async context manager type.

    Should return a redis instance available for the duration of the ACM.

    If it is requested as exclusive, it should be exclusive for the duration of
    the async context manager, and thrown out if any error happens.
    """

    def __call__(
        self, *, master: bool, exclusive: bool
    ) -> AsyncContextManager["Redis"]:
        pass
