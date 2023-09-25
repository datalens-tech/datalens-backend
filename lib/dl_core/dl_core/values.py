from __future__ import annotations

from datetime import (
    date,
    datetime,
)
from typing import (
    ClassVar,
    Generic,
    List,
    TypeVar,
    Union,
)

import attr

from dl_constants.enums import BIType


_INNER_TYPE = TypeVar("_INNER_TYPE")


@attr.s(frozen=True)
class BIValue(Generic[_INNER_TYPE]):
    type: ClassVar[BIType]
    value: _INNER_TYPE = attr.ib()


@attr.s(frozen=True)
class StringValue(BIValue[str]):
    type: ClassVar[BIType] = BIType.string


@attr.s(frozen=True)
class IntegerValue(BIValue[int]):
    type: ClassVar[BIType] = BIType.integer


@attr.s(frozen=True)
class FloatValue(BIValue[float]):
    type: ClassVar[BIType] = BIType.float


@attr.s(frozen=True)
class DateValue(BIValue[date]):
    type: ClassVar[BIType] = BIType.date


@attr.s(frozen=True)
class DateTimeValue(BIValue[datetime]):
    type: ClassVar[BIType] = BIType.datetime


@attr.s(frozen=True)
class DateTimeTZValue(BIValue[datetime]):
    type: ClassVar[BIType] = BIType.datetimetz


@attr.s(frozen=True)
class GenericDateTimeValue(BIValue[datetime]):
    type: ClassVar[BIType] = BIType.genericdatetime


@attr.s(frozen=True)
class BooleanValue(BIValue[bool]):
    type: ClassVar[BIType] = BIType.boolean


@attr.s(frozen=True)
class GeoPointValue(BIValue[List[Union[int, float]]]):
    type: ClassVar[BIType] = BIType.geopoint


@attr.s(frozen=True)
class GeoPolygonValue(BIValue[List[List[List[Union[int, float]]]]]):
    type: ClassVar[BIType] = BIType.geopolygon


@attr.s(frozen=True)
class UuidValue(BIValue[str]):
    type: ClassVar[BIType] = BIType.uuid


@attr.s(frozen=True)
class MarkupValue(BIValue[str]):
    type: ClassVar[BIType] = BIType.markup


@attr.s(frozen=True)
class ArrayStrValue(BIValue[List[str]]):
    type: ClassVar[BIType] = BIType.array_str


@attr.s(frozen=True)
class TreeStrValue(BIValue[List[str]]):
    type: ClassVar[BIType] = BIType.tree_str


@attr.s(frozen=True)
class ArrayIntValue(BIValue[List[int]]):
    type: ClassVar[BIType] = BIType.array_int


@attr.s(frozen=True)
class ArrayFloatValue(BIValue[List[float]]):
    type: ClassVar[BIType] = BIType.array_float
