from __future__ import annotations

from bi_constants.types import (
    TBIDataRow,
    TJSONExt,
)
from bi_core.data_processing.streaming import AsyncChunkedBase

TValuesChunkStream = AsyncChunkedBase[TBIDataRow]  # wrapper of async iterable of chunks of rows
TJSONExtChunkStream = AsyncChunkedBase[TJSONExt]  # wrapper of async iterable of chunks of jsonables
