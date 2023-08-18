from __future__ import annotations

import datetime
import decimal
import uuid
from typing import AsyncGenerator, AsyncIterable, Dict, List, Sequence, Tuple, Union


TJSONScalar = Union[str, float, int, bool, None]
# Often used through `bi_core.data_types.DatalensDataTypes`
TBIDataValue = Union[
    # All the python types that connection executors are known to spit out.
    # (does not include e.g. `memoryview` which should always be processed in the executor).
    datetime.date, datetime.datetime, datetime.time, datetime.timedelta,
    decimal.Decimal, uuid.UUID, bytes,
    TJSONScalar,
]


# Allowing `tuple` as it is "jsonable"
# # Recursive (not supported by mypy):
# TJSONLike = Union[
#     TJSONScalar,
#     Dict[str, 'TJSONLike'],
#     List['TJSONLike'],
#     Tuple['TJSONLike', ...],
# ]
# # Limited recursion:
TJSONLikeUnchecked = Union[
    dict, list, tuple,
    TJSONScalar,
]
TJSONLike = Union[
    Dict[str, TJSONLikeUnchecked],
    List[TJSONLikeUnchecked],
    Tuple[TJSONLikeUnchecked, ...],
    TJSONScalar,
]


# Types supported by the extended JSON serializer (`bi_core.serialization`)
# # Recursive (not supported by mypy):
# TJSONExt = Union[
#     Dict[str, 'TJSONExt'],
#     List['TJSONExt']
#     Tuple['TJSONExt', ...],
#     TBIDataValue,
# ]
# # Limited recursion:
TJSONExtUnchecked = Union[
    dict, list, tuple,
    TBIDataValue,
]
TJSONExt = Union[
    Dict[str, TJSONExtUnchecked],
    List[TJSONExtUnchecked],
    Tuple[TJSONExtUnchecked, ...],
    TBIDataValue,
]


# More convenience shortcuts
TJSONLikeAIter = AsyncIterable[TJSONLike]
TJSONExtAIter = AsyncIterable[TJSONExt]
TBIDataRow = Sequence[TBIDataValue]  # sequence of cells -> row
TBIDataTable = Sequence[TBIDataRow]  # sequence of rows -> table
TBIChunksGen = AsyncGenerator[TBIDataTable, None]  # sequence of tables -> chunks
# NOTE: ^ often wrapped in an AsyncChunked


# Compatibility aliases:
# FIXME: mass-rename (and move usages to `TYPE_CHECKING` when possible).
JSONScalarTypes = TJSONScalar
BIDataTypes = TBIDataValue
# DatalensDataTypes = BIDataTypes  # see `.data_types`
