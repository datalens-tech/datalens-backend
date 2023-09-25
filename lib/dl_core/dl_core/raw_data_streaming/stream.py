from __future__ import annotations

from abc import ABCMeta
from typing import (
    AsyncIterator,
    Iterator,
    Optional,
    TypeVar,
    Union,
)


_ITER_TV = TypeVar("_ITER_TV")


class DataStreamBase(Iterator[_ITER_TV], metaclass=ABCMeta):
    max_percent = 99

    def __iter__(self):  # type: ignore  # TODO: fix
        return self

    def get_progress_percent(self) -> int:
        """Return current progress in percent"""
        return self.max_percent

    def __next__(self) -> _ITER_TV:
        raise StopIteration


class AsyncDataStreamBase(AsyncIterator[_ITER_TV], metaclass=ABCMeta):
    max_percent = 99

    def __aiter__(self) -> AsyncDataStreamBase:
        return self

    def get_progress_percent(self) -> int:
        """Return current progress in percent"""
        return self.max_percent

    async def __anext__(self) -> _ITER_TV:
        raise StopIteration


class SimpleDataStream(DataStreamBase):
    """
    Data stream that calculates progress percentage based on total data size.
    """

    def __init__(self, data_iter: Iterator[dict], rows_to_copy: Optional[int] = None):
        self._data_iter = data_iter
        self._rows_to_copy = rows_to_copy

        self._rows_read = 0

    def get_progress_percent(self) -> int:
        if self._rows_to_copy is None:
            return 0
        if self._rows_to_copy == 0:
            return 100
        return min(self.max_percent, int((self._rows_read / self._rows_to_copy) * 100))

    def __next__(self) -> dict:
        row = next(self._data_iter)
        self._rows_read += 1
        return row


class LazyDataStream(DataStreamBase):
    """
    Data stream with progress percentage set from the outside.
    Does no calculations by itself.
    """

    def __init__(self, data_iter: Iterator[dict]):
        self._data_iter = data_iter

        self._progress = 0

    def set_progress_percent(self, progress: Union[int, float]) -> None:
        self._progress = int(progress)

    def get_progress_percent(self) -> int:
        return self._progress

    def __next__(self) -> dict:
        row = next(self._data_iter)
        return row


class SimpleUntypedDataStream(DataStreamBase):
    """
    Data stream that calculates progress percentage based on total data size.
    """

    def __init__(self, data_iter: Iterator[list], rows_to_copy: Optional[int] = None):
        self._data_iter = data_iter
        self._rows_to_copy = rows_to_copy

        self._rows_read = 0

    def get_progress_percent(self) -> int:
        if self._rows_to_copy is None:
            return 0
        if self._rows_to_copy == 0:
            return 100
        return min(self.max_percent, int((self._rows_read / self._rows_to_copy) * 100))

    def __next__(self) -> list:
        row = next(self._data_iter)
        self._rows_read += 1
        return row


class SimpleUntypedAsyncDataStream(AsyncDataStreamBase):
    def __init__(self, data_iter: AsyncIterator[list], rows_to_copy: Optional[int] = None):
        self._data_iter = data_iter
        self._rows_to_copy = rows_to_copy

        self._rows_read = 0

    def get_progress_percent(self) -> int:
        if self._rows_to_copy is None:
            return 0
        if self._rows_to_copy == 0:
            return 100
        return min(self.max_percent, int((self._rows_read / self._rows_to_copy) * 100))

    async def __anext__(self) -> list:
        row = await self._data_iter.__anext__()
        self._rows_read += 1
        return row
