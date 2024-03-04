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

from dl_constants.enums import UserDataType


_INNER_TYPE = TypeVar("_INNER_TYPE")


@attr.s(frozen=True)
class BIValue(Generic[_INNER_TYPE]):
    type: ClassVar[UserDataType]
    value: _INNER_TYPE = attr.ib()


@attr.s(frozen=True)
class StringValue(BIValue[str]):
    type: ClassVar[UserDataType] = UserDataType.string


@attr.s(frozen=True)
class IntegerValue(BIValue[int]):
    type: ClassVar[UserDataType] = UserDataType.integer


@attr.s(frozen=True)
class FloatValue(BIValue[float]):
    type: ClassVar[UserDataType] = UserDataType.float


@attr.s(frozen=True)
class DateValue(BIValue[date]):
    type: ClassVar[UserDataType] = UserDataType.date


@attr.s(frozen=True)
class DateTimeValue(BIValue[datetime]):
    type: ClassVar[UserDataType] = UserDataType.datetime


@attr.s(frozen=True)
class DateTimeTZValue(BIValue[datetime]):
    type: ClassVar[UserDataType] = UserDataType.datetimetz


@attr.s(frozen=True)
class GenericDateTimeValue(BIValue[datetime]):
    type: ClassVar[UserDataType] = UserDataType.genericdatetime


@attr.s(frozen=True)
class BooleanValue(BIValue[bool]):
    type: ClassVar[UserDataType] = UserDataType.boolean


@attr.s(frozen=True)
class GeoPointValue(BIValue[List[Union[int, float]]]):
    type: ClassVar[UserDataType] = UserDataType.geopoint


@attr.s(frozen=True)
class GeoPolygonValue(BIValue[List[List[List[Union[int, float]]]]]):
    type: ClassVar[UserDataType] = UserDataType.geopolygon


@attr.s(frozen=True)
class UuidValue(BIValue[str]):
    type: ClassVar[UserDataType] = UserDataType.uuid


@attr.s(frozen=True)
class MarkupValue(BIValue[str]):
    type: ClassVar[UserDataType] = UserDataType.markup


@attr.s(frozen=True)
class ArrayStrValue(BIValue[List[str]]):
    type: ClassVar[UserDataType] = UserDataType.array_str


@attr.s(frozen=True)
class TreeStrValue(BIValue[List[str]]):
    type: ClassVar[UserDataType] = UserDataType.tree_str


@attr.s(frozen=True)
class ArrayIntValue(BIValue[List[int]]):
    type: ClassVar[UserDataType] = UserDataType.array_int


@attr.s(frozen=True)
class ArrayFloatValue(BIValue[List[float]]):
    type: ClassVar[UserDataType] = UserDataType.array_float
