from __future__ import annotations

from dl_constants.types import TBIDataRow
from dl_utils.streaming import AsyncChunkedBase


TValuesChunkStream = AsyncChunkedBase[TBIDataRow]  # wrapper of async iterable of chunks of rows
