from __future__ import annotations

from collections import OrderedDict
from enum import (
    Enum,
    unique,
)
from typing import (
    Hashable,
    Optional,
)

import attr

from dl_formula.core import exc


@unique
class DataType(Enum):
    NULL = "null"
    INTEGER = "integer"
    CONST_INTEGER = "const_integer"
    FLOAT = "float"
    CONST_FLOAT = "const_float"
    STRING = "string"
    CONST_STRING = "const_string"
    DATE = "date"
    CONST_DATE = "const_date"
    DATETIME = "datetime"
    CONST_DATETIME = "const_datetime"
    DATETIMETZ = "datetimetz"  # timezone-aware datetime: UTC datetime + timezone name, normally.
    CONST_DATETIMETZ = "const_datetimetz"
    GENERICDATETIME = "genericdatetime"  # datetime with no special timezone logic
    CONST_GENERICDATETIME = "const_genericdatetime"
    BOOLEAN = "boolean"
    CONST_BOOLEAN = "const_boolean"
    GEOPOINT = "geopoint"
    CONST_GEOPOINT = "const_geopoint"
    GEOPOLYGON = "geopolygon"
    CONST_GEOPOLYGON = "const_geopolygon"
    # "MARKUP": a specially-structured string that contains a markup tree.
    MARKUP = "markup"
    CONST_MARKUP = "const_markup"
    UUID = "uuid"
    CONST_UUID = "const_uuid"
    ARRAY_FLOAT = "array_float"
    CONST_ARRAY_FLOAT = "const_array_float"
    ARRAY_INT = "array_int"
    CONST_ARRAY_INT = "const_array_int"
    ARRAY_STR = "array_str"
    CONST_ARRAY_STR = "const_array_str"
    TREE_STR = "tree_str"
    CONST_TREE_STR = "const_tree_str"
    # 'unsupported type', for all unlisted database types. Should be
    # *explicitly* stringifiable. Should not ever be a constant.
    # Somewhat similar to DataType.NULL, but makes it possible to extract some value.
    UNSUPPORTED = "unsupported"

    @classmethod
    def get_common_cast_type(cls, *types: DataType) -> DataType:
        set_of_types = set(types)
        if len(set_of_types) == 1:
            return set_of_types.pop()

        for to_type, from_types in _AUTOCAST_FROM_TYPES.items():
            if set_of_types.issubset(from_types):
                return to_type

        raise exc.TypeConflictError("Type conflict: [{}]".format(", ".join(sorted(t.name for t in types))))

    def casts_to(self, type_: DataType) -> bool:
        """Check whether ``self`` can be automatically cast to ``type_``"""
        return self in _AUTOCAST_FROM_TYPES[type_]

    @property
    def is_const(self) -> bool:
        """Flag indicating whether the type is constant"""
        return self.value.startswith("const_")

    @property
    def non_const_type(self) -> DataType:
        """Return a non-constant version of the type"""
        return self.__class__[self.name.replace("CONST_", "")]

    @property
    def const_type(self) -> DataType:
        """Return a constant version of the type"""
        if self.is_const or self is DataType.NULL or self is DataType.UNSUPPORTED:
            return self
        return DataType["CONST_" + self.name]

    @property
    def autocast_types(self) -> frozenset[DataType]:
        return frozenset(_AUTOCAST_FROM_TYPES.get(self, ()))


@attr.s(frozen=True)
class DataTypeParams:
    """Mix of all possible parameters for parametrized data types"""

    timezone: Optional[str] = attr.ib(default=None)
    # Other possible cases: decimal precision, datetime sub-second precision, nullable, enum values.

    def as_primitive(self) -> tuple[Optional[Hashable]]:
        return (self.timezone,)


_AUTOCAST_FROM_TYPES = OrderedDict(
    (
        # <to_type>: set(<from_types>)
        # ordered from more specific (fewer from_types) to less specific (more from_types) type casts
        (DataType.NULL, {DataType.NULL}),
        (DataType.CONST_INTEGER, {DataType.CONST_INTEGER}),
        (DataType.CONST_DATE, {DataType.CONST_DATE}),
        (DataType.CONST_STRING, {DataType.CONST_STRING}),
        (DataType.CONST_BOOLEAN, {DataType.CONST_BOOLEAN}),
        (DataType.CONST_DATETIME, {DataType.CONST_DATETIME}),
        (DataType.CONST_DATETIMETZ, {DataType.CONST_DATETIMETZ}),
        (DataType.CONST_GENERICDATETIME, {DataType.CONST_GENERICDATETIME}),
        (DataType.CONST_FLOAT, {DataType.CONST_FLOAT, DataType.CONST_INTEGER}),
        (DataType.CONST_GEOPOINT, {DataType.CONST_GEOPOINT}),
        (DataType.CONST_GEOPOLYGON, {DataType.CONST_GEOPOLYGON}),
        (DataType.CONST_MARKUP, {DataType.CONST_MARKUP}),
        (DataType.CONST_UUID, {DataType.CONST_UUID}),
        (DataType.CONST_ARRAY_FLOAT, {DataType.CONST_ARRAY_FLOAT}),
        (DataType.CONST_ARRAY_INT, {DataType.CONST_ARRAY_INT}),
        (DataType.CONST_ARRAY_STR, {DataType.CONST_ARRAY_STR}),
        (DataType.CONST_TREE_STR, {DataType.CONST_TREE_STR}),
        (DataType.INTEGER, {DataType.INTEGER, DataType.CONST_INTEGER, DataType.NULL}),
        (DataType.DATE, {DataType.DATE, DataType.CONST_DATE, DataType.NULL}),
        (DataType.STRING, {DataType.STRING, DataType.CONST_STRING, DataType.NULL}),
        (DataType.GEOPOINT, {DataType.GEOPOINT, DataType.CONST_GEOPOINT, DataType.NULL}),
        (DataType.GEOPOLYGON, {DataType.GEOPOLYGON, DataType.CONST_GEOPOLYGON, DataType.NULL}),
        (DataType.MARKUP, {DataType.MARKUP, DataType.CONST_MARKUP, DataType.NULL}),
        (DataType.BOOLEAN, {DataType.BOOLEAN, DataType.CONST_BOOLEAN, DataType.NULL}),
        (DataType.DATETIME, {DataType.DATETIME, DataType.CONST_DATETIME, DataType.NULL}),
        (DataType.DATETIMETZ, {DataType.DATETIMETZ, DataType.CONST_DATETIMETZ, DataType.NULL}),
        (DataType.GENERICDATETIME, {DataType.GENERICDATETIME, DataType.CONST_GENERICDATETIME, DataType.NULL}),
        (
            DataType.FLOAT,
            {DataType.FLOAT, DataType.INTEGER, DataType.CONST_FLOAT, DataType.CONST_INTEGER, DataType.NULL},
        ),
        (DataType.UUID, {DataType.UUID, DataType.CONST_UUID, DataType.NULL}),
        (DataType.TREE_STR, {DataType.TREE_STR, DataType.CONST_TREE_STR, DataType.NULL}),
        (DataType.ARRAY_FLOAT, {DataType.ARRAY_FLOAT, DataType.CONST_ARRAY_FLOAT, DataType.NULL}),
        (DataType.ARRAY_INT, {DataType.ARRAY_INT, DataType.CONST_ARRAY_INT, DataType.NULL}),
        (
            DataType.ARRAY_STR,
            {
                DataType.ARRAY_STR,
                DataType.CONST_ARRAY_STR,
                DataType.TREE_STR,
                DataType.CONST_TREE_STR,
                DataType.NULL,
            },
        ),
        (DataType.UNSUPPORTED, {DataType.UNSUPPORTED}),
    )
)
