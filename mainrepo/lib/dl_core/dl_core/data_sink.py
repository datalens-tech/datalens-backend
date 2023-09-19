from __future__ import annotations

import abc
import logging
from typing import (
    Any,
    Generic,
    List,
    Optional,
    Type,
    TypeVar,
)

from dl_core.db.elements import SchemaColumn
from dl_core.raw_data_streaming.stream import DataStreamBase


LOGGER = logging.getLogger(__name__)


class DataSink:
    """Object that accepts data in batches and stores it somewhere"""

    def __init__(self, bi_schema: List[SchemaColumn]):
        self._bi_schema = bi_schema

    def initialize(self) -> None:
        """Create table, start external service, etc. - whatever is needed to get it running"""

    def dump_data_stream(self, data_stream: DataStreamBase) -> None:
        """Send stream of entries (list of dicts) to storage"""

    def finalize(self) -> None:
        """Finalize materialization"""

    def cleanup(self) -> None:
        """Cleanup tmp data"""

    def close(self) -> None:
        """Close conn executor and all closable entities"""

    def __enter__(self) -> DataSink:
        self.initialize()
        return self

    def __exit__(self, exc_type: Optional[Type[Exception]], exc_val: Optional[Exception], exc_tb: Any) -> None:
        if exc_type is None:
            self.finalize()
        else:
            LOGGER.exception("Exception occurred in DataSink")
        self.cleanup()
        self.close()


_DATA_STREAM_TV = TypeVar("_DATA_STREAM_TV")


class DataSinkAsync(Generic[_DATA_STREAM_TV], metaclass=abc.ABCMeta):
    """Object that accepts data in batches and stores it somewhere"""

    async def initialize(self) -> None:
        """Create table, start external service, etc. - whatever is needed to get it running"""

    async def dump_data_stream(self, data_stream: _DATA_STREAM_TV) -> None:
        """Send stream of entries (list of dicts) to storage"""

    async def finalize(self) -> None:
        """Finalize materialization"""

    async def cleanup(self) -> None:
        """Cleanup tmp data"""

    async def close(self) -> None:
        """Close conn executor and all closable entities"""

    async def __aenter__(self) -> DataSinkAsync:
        await self.initialize()
        return self

    async def __aexit__(self, exc_type: Optional[Type[Exception]], exc_val: Optional[Exception], exc_tb: Any) -> None:
        if exc_type is None:
            await self.finalize()
        await self.cleanup()
        await self.close()
