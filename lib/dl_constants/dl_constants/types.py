from __future__ import annotations

from collections.abc import (
    AsyncGenerator,
    AsyncIterable,
    Sequence,
)
import datetime
import decimal
import ipaddress
import uuid

TJSONScalar = str | float | int | bool | None
TBIDataValue = (
    datetime.date
    | datetime.datetime
    | datetime.time
    | datetime.timedelta
    | decimal.Decimal
    | uuid.UUID
    | bytes
    | TJSONScalar
    | ipaddress.IPv4Address
    | ipaddress.IPv6Address
    | ipaddress.IPv4Network
    | ipaddress.IPv6Network
    | ipaddress.IPv4Interface
    | ipaddress.IPv6Interface
)


# Allowing `tuple` as it is "jsonable"
# # Recursive (not supported by mypy):
# TJSONLike = (
#     TJSONScalar
#     | dict[str, 'TJSONLike']
#     | list['TJSONLike']
#     | tuple['TJSONLike', ...]
# )
# # Limited recursion:
TJSONLikeUnchecked = dict | list | tuple | TJSONScalar
TJSONLike = dict[str, TJSONLikeUnchecked] | list[TJSONLikeUnchecked] | tuple[TJSONLikeUnchecked, ...] | TJSONScalar


# Types supported by the extended JSON serializer (`bi_core.serialization`)
# # Recursive (not supported by mypy):
# TJSONExt = (
#     dict[str, 'TJSONExt']
#     | list['TJSONExt']
#     | tuple['TJSONExt', ...]
#     | TBIDataValue
# )
# # Limited recursion:
TJSONExtUnchecked = dict | list | tuple | TBIDataValue
TJSONExt = dict[str, TJSONExtUnchecked] | list[TJSONExtUnchecked] | tuple[TJSONExtUnchecked, ...] | TBIDataValue


# More convenience shortcuts
TJSONLikeAIter = AsyncIterable[TJSONLike]
TJSONExtAIter = AsyncIterable[TJSONExt]
TBIDataRow = Sequence[TBIDataValue]  # sequence of cells -> row
TBIDataTable = Sequence[TBIDataRow]  # sequence of rows -> table
TBIChunksGen = AsyncGenerator[TBIDataTable, None]  # sequence of tables -> chunks
# NOTE: ^ often wrapped in an AsyncChunked
