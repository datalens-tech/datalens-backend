"""Type stubs for dl_utils.streaming module"""

from typing import (
    AsyncIterator,
    Generic,
    List,
    TypeVar,
)

T = TypeVar("T")

class AsyncChunkedBase(Generic[T]):
    async def all(self) -> List[T]: ...
    async def __aiter__(self) -> AsyncIterator[T]: ...

class AsyncChunked(AsyncChunkedBase[T]):
    @classmethod
    def from_chunked_iterable(cls, iterable: List[List[T]]) -> "AsyncChunked[T]": ...
    async def all(self) -> List[T]: ...
    async def __aiter__(self) -> AsyncIterator[T]: ...
