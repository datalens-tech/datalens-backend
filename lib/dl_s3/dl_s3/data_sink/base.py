from __future__ import annotations

import abc
import logging
from typing import (
    Any,
    Generic,
    Optional,
    TypeVar,
)

from dl_s3.stream import (
    AsyncDataStreamBase,
    DataStreamBase,
)


LOGGER = logging.getLogger(__name__)


_DATA_STREAM_TV = TypeVar("_DATA_STREAM_TV", bound=DataStreamBase)


class DataSink(Generic[_DATA_STREAM_TV], metaclass=abc.ABCMeta):
    """Object that accepts data in batches and stores it somewhere"""

    @abc.abstractmethod
    def initialize(self) -> None:
        """Create table, start external service, etc. - whatever is needed to get it running"""

    @abc.abstractmethod
    def dump_data_stream(self, data_stream: _DATA_STREAM_TV) -> None:
        """Send stream of entries (list of dicts) to storage"""

    @abc.abstractmethod
    def finalize(self) -> None:
        """Finalize materialization"""

    @abc.abstractmethod
    def cleanup(self) -> None:
        """Cleanup tmp data"""

    def __enter__(self) -> DataSink:
        self.initialize()
        return self

    def __exit__(self, exc_type: Optional[type[Exception]], exc_val: Optional[Exception], exc_tb: Any) -> None:
        if exc_type is None:
            self.finalize()
        else:
            LOGGER.exception("Exception occurred in DataSink")
        self.cleanup()


_ASYNC_DATA_STREAM_TV = TypeVar("_ASYNC_DATA_STREAM_TV", bound=AsyncDataStreamBase)


class DataSinkAsync(Generic[_ASYNC_DATA_STREAM_TV], metaclass=abc.ABCMeta):
    """Object that accepts data in batches and stores it somewhere"""

    @abc.abstractmethod
    async def initialize(self) -> None:
        """Create table, start external service, etc. - whatever is needed to get it running"""

    @abc.abstractmethod
    async def dump_data_stream(self, data_stream: _ASYNC_DATA_STREAM_TV) -> None:
        """Send stream of entries (list of dicts) to storage"""

    @abc.abstractmethod
    async def finalize(self) -> None:
        """Finalize materialization"""

    @abc.abstractmethod
    async def cleanup(self) -> None:
        """Cleanup tmp data"""

    async def __aenter__(self) -> DataSinkAsync:
        await self.initialize()
        return self

    async def __aexit__(self, exc_type: Optional[type[Exception]], exc_val: Optional[Exception], exc_tb: Any) -> None:
        if exc_type is None:
            await self.finalize()
        await self.cleanup()
