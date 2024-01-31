from __future__ import annotations

import datetime
import re
from typing import (
    Any,
    Optional,
)

from clickhouse_sqlalchemy import types as ch_types

from dl_constants.enums import UserDataType
from dl_core.db.conversion_base import (
    BooleanTypeCaster,
    DateTypeCaster,
    TypeCaster,
    make_datetime,
    make_int,
    make_native_type,
)

from dl_connector_clickhouse.core.clickhouse_base.constants import CONNECTION_TYPE_CLICKHOUSE
from dl_connector_clickhouse.core.clickhouse_base.type_transformer import ClickHouseTypeTransformer


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


def _make_datetime(value: Any) -> Optional[datetime.datetime]:
    # should parse formats like:
    # %Y-%m-%d %H:%M:%S.%f
    # %Y-%m-%d %H:%M:%S.%f+timezone
    # %Y-%m-%dT%H:%M:%S.%f+timezone
    # %Y-%m-%dT%H:%M:%S.%f
    # %d.%m.%Y %H:%M:%S.%f
    # see dl_file_uploader_worker_lib.utils.converter_parsing_utils._check_datetime_re

    if isinstance(value, str):
        for f in ("%d.%m.%Y %H:%M:%S.%f", "%d.%m.%Y %H:%M:%S"):
            try:
                dt = datetime.datetime.strptime(value, f)
            except ValueError:
                pass
            else:
                return dt

    return make_datetime(value)


class IntegerFileTypeCaster(TypeCaster):
    def _cast_for_input(self, value: Any) -> Any:
        return make_int_cleanup_spaces(value)


class FloatFileTypeCaster(TypeCaster):
    def _cast_for_input(self, value: Any) -> Any:
        return make_float_cleanup_spaces(value)


class DateFileTypeCaster(DateTypeCaster):
    def _cast_for_input(self, value: Any) -> Any:
        return DateTypeCaster.cast_func(value).isoformat()  # type: ignore  # 2024-01-30 # TODO: Item "None" of "date | None" has no attribute "isoformat"  [union-attr]


class DatetimeFileCommonTypeCaster(TypeCaster):
    cast_func = _make_datetime


class DatetimeFileTypeCaster(DatetimeFileCommonTypeCaster):
    def _cast_for_input(self, value: Any) -> Any:
        dt: Optional[datetime.datetime] = DatetimeFileCommonTypeCaster.cast_func(value)
        if dt is not None:
            if dt.tzinfo is not None and dt.utcoffset() is not None:
                dt = dt.replace(tzinfo=None) - dt.utcoffset()  # type: ignore  # 2024-01-30 # TODO: No overload variant of "__sub__" of "datetime" matches argument type "None"  [operator]
            return dt.isoformat()
        return None


class DatetimeTZFileTypeCaster(DatetimeFileCommonTypeCaster):
    def _cast_for_input(self, value: Any) -> Any:
        return None if (dt := DatetimeFileCommonTypeCaster.cast_func(value)) is None else dt.isoformat()


class GenericDatetimeFileTypeCaster(DatetimeFileCommonTypeCaster):
    def _cast_for_input(self, value: Any) -> Any:
        dt: Optional[datetime.datetime] = DatetimeFileCommonTypeCaster.cast_func(value)
        if dt is not None:
            if dt.tzinfo is not None and dt.utcoffset() is not None:
                dt = dt.replace(tzinfo=None) - dt.utcoffset()  # type: ignore  # 2024-01-30 # TODO: No overload variant of "__sub__" of "datetime" matches argument type "None"  [operator]
            return dt.isoformat()
        return None


class BooleanFileTypeCaster(BooleanTypeCaster):
    cast_func = make_boolean


class FileTypeTransformer(ClickHouseTypeTransformer):
    casters = {
        **ClickHouseTypeTransformer.casters,
        UserDataType.integer: IntegerFileTypeCaster(),
        UserDataType.float: FloatFileTypeCaster(),
        UserDataType.date: DateFileTypeCaster(),
        UserDataType.datetime: DatetimeFileTypeCaster(),
        UserDataType.datetimetz: DatetimeTZFileTypeCaster(),
        UserDataType.genericdatetime: GenericDatetimeFileTypeCaster(),
        UserDataType.boolean: BooleanFileTypeCaster(),
    }

    user_to_native_map = {
        **ClickHouseTypeTransformer.user_to_native_map,
        UserDataType.datetime: make_native_type(ch_types.DateTime64),
        UserDataType.genericdatetime: make_native_type(ch_types.DateTime64),
        UserDataType.date: make_native_type(ch_types.Date32),
    }

    @classmethod
    def cast_for_input(cls, value: Any, user_t: UserDataType) -> Any:
        """Prepare value for insertion into the database"""
        if value == "" or value is None:
            return None
        return cls.casters[user_t].cast_for_input(value=value)
