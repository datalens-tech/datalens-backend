from __future__ import annotations

import datetime
import decimal
from typing import (
    AsyncGenerator,
    AsyncIterable,
    Sequence,
    Union,
)
import uuid


TJSONScalar = Union[str, float, int, bool, None]
TBIDataValue = Union[
    # All the python types that connection executors are known to spit out.
    # (does not include e.g. `memoryview` which should always be processed in the executor).
    datetime.date,
    datetime.datetime,
    datetime.time,
    datetime.timedelta,
    decimal.Decimal,
    uuid.UUID,
    bytes,
    TJSONScalar,
]


# Allowing `tuple` as it is "jsonable"
# # Recursive (not supported by mypy):
# TJSONLike = Union[
#     TJSONScalar,
#     dict[str, 'TJSONLike'],
#     list['TJSONLike'],
#     tuple['TJSONLike', ...],
# ]
# # Limited recursion:
TJSONLikeUnchecked = Union[
    dict,
    list,
    tuple,
    TJSONScalar,
]
TJSONLike = Union[
    dict[str, TJSONLikeUnchecked],
    list[TJSONLikeUnchecked],
    tuple[TJSONLikeUnchecked, ...],
    TJSONScalar,
]


# Types supported by the extended JSON serializer (`bi_core.serialization`)
# # Recursive (not supported by mypy):
# TJSONExt = Union[
#     dict[str, 'TJSONExt'],
#     list['TJSONExt']
#     tuple['TJSONExt', ...],
#     TBIDataValue,
# ]
# # Limited recursion:
TJSONExtUnchecked = Union[
    dict,
    list,
    tuple,
    TBIDataValue,
]
TJSONExt = Union[
    dict[str, TJSONExtUnchecked],
    list[TJSONExtUnchecked],
    tuple[TJSONExtUnchecked, ...],
    TBIDataValue,
]


# More convenience shortcuts
TJSONLikeAIter = AsyncIterable[TJSONLike]
TJSONExtAIter = AsyncIterable[TJSONExt]
TBIDataRow = Sequence[TBIDataValue]  # sequence of cells -> row
TBIDataTable = Sequence[TBIDataRow]  # sequence of rows -> table
TBIChunksGen = AsyncGenerator[TBIDataTable, None]  # sequence of tables -> chunks
# NOTE: ^ often wrapped in an AsyncChunked
