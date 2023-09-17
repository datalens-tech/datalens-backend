from __future__ import annotations

import datetime
import re
from typing import (
    Any,
    Optional,
)

from clickhouse_sqlalchemy import types as ch_types

from bi_constants.enums import BIType
from bi_core.db.conversion_base import (
    BooleanTypeCaster,
    DatetimeTypeCaster,
    DatetimeTZTypeCaster,
    DateTypeCaster,
    GenericDatetimeTypeCaster,
    TypeCaster,
    make_int,
    make_native_type,
)

from bi_connector_clickhouse.core.clickhouse_base.type_transformer import ClickHouseTypeTransformer
from bi_connector_clickhouse.core.clickhouse_base.constants import CONNECTION_TYPE_CLICKHOUSE


def make_int_cleanup_spaces(value: Any) -> Optional[int]:
    if isinstance(value, str):
        if " " in value:
            value = value.replace(" ", "")
    return make_int(value)


_spaces_in_numbers_re = re.compile(r"(?<=\d) (?=\d)")


def make_float_cleanup_spaces(value: Any) -> Optional[float]:
    if isinstance(value, str):
        if "," in value:
            value = value.replace(",", ".")
        if " " in value:
            value = _spaces_in_numbers_re.sub("", value)
    return float(value)


def make_boolean(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, str):
        value = value.lower()
        if value in ("true", "1"):
            return True
        elif value in ("false", "0"):
            return False
    return bool(value)


class IntegerFileTypeCaster(TypeCaster):
    def _cast_for_input(self, value: Any) -> Any:
        return make_int_cleanup_spaces(value)


class FloatFileTypeCaster(TypeCaster):
    def _cast_for_input(self, value: Any) -> Any:
        return make_float_cleanup_spaces(value)


class DateFileTypeCaster(DateTypeCaster):
    def _cast_for_input(self, value: Any) -> Any:
        return DateTypeCaster.cast_func(value).isoformat()  # type: ignore


class DatetimeFileTypeCaster(DatetimeTypeCaster):
    def _cast_for_input(self, value: Any) -> Any:
        dt: Optional[datetime.datetime] = DatetimeTypeCaster.cast_func(value)
        if dt is not None:
            if dt.tzinfo is not None and dt.utcoffset() is not None:
                dt = dt.replace(tzinfo=None) - dt.utcoffset()  # type: ignore
            return dt.isoformat()
        return None


class DatetimeTZFileTypeCaster(DatetimeTZTypeCaster):
    def _cast_for_input(self, value: Any) -> Any:
        return DatetimeTZTypeCaster.cast_func(value).isoformat()  # type: ignore


class GenericDatetimeFileTypeCaster(GenericDatetimeTypeCaster):
    def _cast_for_input(self, value: Any) -> Any:
        dt: Optional[datetime.datetime] = GenericDatetimeTypeCaster.cast_func(value)
        if dt is not None:
            if dt.tzinfo is not None and dt.utcoffset() is not None:
                dt = dt.replace(tzinfo=None) - dt.utcoffset()  # type: ignore
            return dt.isoformat()
        return None


class BooleanFileTypeCaster(BooleanTypeCaster):
    cast_func = make_boolean


class FileTypeTransformer(ClickHouseTypeTransformer):
    casters = {
        **ClickHouseTypeTransformer.casters,
        BIType.integer: IntegerFileTypeCaster(),
        BIType.float: FloatFileTypeCaster(),
        BIType.date: DateFileTypeCaster(),
        BIType.datetime: DatetimeFileTypeCaster(),
        BIType.datetimetz: DatetimeTZFileTypeCaster(),
        BIType.genericdatetime: GenericDatetimeFileTypeCaster(),
        BIType.boolean: BooleanFileTypeCaster(),
    }

    user_to_native_map = {
        **ClickHouseTypeTransformer.user_to_native_map,
        BIType.datetime: make_native_type(CONNECTION_TYPE_CLICKHOUSE, ch_types.DateTime64),
        BIType.genericdatetime: make_native_type(CONNECTION_TYPE_CLICKHOUSE, ch_types.DateTime64),
        BIType.date: make_native_type(CONNECTION_TYPE_CLICKHOUSE, ch_types.Date32),
    }

    @classmethod
    def cast_for_input(cls, value: Any, user_t: BIType) -> Any:
        """Prepare value for insertion into the database"""
        if value == "" or value is None:
            return None
        return cls.casters[user_t].cast_for_input(value=value)
