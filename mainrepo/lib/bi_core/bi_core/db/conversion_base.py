from __future__ import annotations

from contextlib import contextmanager
import datetime
from functools import (
    lru_cache,
    partial,
)
import math
from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    Generator,
    Iterable,
    Optional,
    Type,
    TypeVar,
    final,
)

import pytz

from bi_constants.enums import (
    BIType,
    ConnectionType,
)
from bi_core import (
    converter_types_cast,
    exc,
)
from bi_core.db.native_type import (
    CommonNativeType,
    GenericNativeType,
)

make_native_type = GenericNativeType.normalize_name_and_create


_INN_TV = TypeVar("_INN_TV")


def _if_not_none(value: _INN_TV, func: Callable[[_INN_TV], Any]) -> Any:
    if value is not None:
        try:
            return func(value)
        except ValueError:
            raise exc.DataParseError(f"Cannot convert {value!r} to {func.__name__}", query=None)
    return None


def make_date(value: Any) -> Optional[datetime.date]:
    if isinstance(value, datetime.datetime):
        return value.date()
    if isinstance(value, datetime.date):
        return value
    if isinstance(value, str):
        return converter_types_cast._to_date(value)
    return None


def make_datetime(value: Any) -> Optional[datetime.datetime]:
    if isinstance(value, datetime.datetime):
        return value
    if isinstance(value, datetime.date):
        return datetime.datetime(value.year, value.month, value.day)
    if isinstance(value, str):
        return converter_types_cast._to_datetime(value)
    return None


def make_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, float) and (math.isinf(value) or math.isnan(value)):
        return None
    return int(value)


@contextmanager
def _handle_type_cast_errors() -> Generator[None, None, None]:
    try:
        yield
    except (TypeError, ValueError):
        raise exc.TypeCastFailed("Type casting failed for value")


class TypeCaster:
    cast_func: Callable[[Any], Any] = lambda value: value
    custom_null_value: Any = None

    def _cast_for_input(self, value: Any) -> Any:
        if self.custom_null_value is not None:
            if value is None or value == self.custom_null_value:
                return self.custom_null_value
        return self.__class__.cast_func(value)

    def _cast_for_output(self, value: Any) -> Any:
        if self.custom_null_value is not None:
            if value is None or value == self.custom_null_value:
                return None
        return self.__class__.cast_func(value)

    @final
    def cast_for_input(self, value: Any) -> Any:
        with _handle_type_cast_errors():
            return self._cast_for_input(value)

    @final
    def cast_for_output(self, value: Any) -> Any:
        with _handle_type_cast_errors():
            return self._cast_for_output(value)


class IntegerTypeCaster(TypeCaster):
    cast_func = make_int


class FloatTypeCaster(TypeCaster):
    cast_func = lambda value: _if_not_none(value, float)  # noqa


class BooleanTypeCaster(TypeCaster):
    cast_func = lambda value: _if_not_none(value, bool)  # noqa


class YTBooleanTypeCaster(BooleanTypeCaster):
    def _cast_for_output(self, value: Any) -> Optional[bool]:
        if value == "0":
            return False
        elif value == "1":
            return True
        else:
            return super()._cast_for_output(value)


class StringTypeCaster(TypeCaster):
    cast_func = lambda value: _if_not_none(value, str)  # noqa


class DateTypeCaster(TypeCaster):
    cast_func = make_date


class DatetimeTypeCaster(TypeCaster):
    cast_func = make_datetime


class DatetimeTZTypeCaster(TypeCaster):
    cast_func = make_datetime  # TODO: re-check edgy cases


class GenericDatetimeTypeCaster(TypeCaster):
    cast_func = make_datetime


class UTCDatetimeTypeCaster(TypeCaster):
    def _cast_for_input(self, value: Any) -> Optional[datetime.datetime]:
        result = super()._cast_for_output(value)
        if isinstance(result, datetime.datetime) and result.tzinfo is not None:
            result = result.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        return result


class UTCTimezoneDatetimeTypeCaster(TypeCaster):
    def _cast_for_input(self, value: Any) -> Optional[datetime.datetime]:
        result = super()._cast_for_output(value)
        if isinstance(result, datetime.datetime) and result.tzinfo is None:
            result = result.replace(tzinfo=pytz.UTC)
        return result


class UnsupportedCaster(TypeCaster):
    """
    Only allows `NULL`s.
    See also: `bi_core.data_source.sql.BaseSQLDataSource._make_raw_column_select`
    """

    def _cast_for_input(self, value: Any) -> None:
        if value is None:
            return None
        raise exc.TypeCastUnsupported("Asked `cast_for_input` for an Unsupported type for a non-null")

    def _cast_for_output(self, value: Any) -> None:
        if value is None:
            return None
        raise exc.TypeCastUnsupported("Asked `cast_for_output` for an Unsupported type for a non-null")


def _cast_array(value: Optional[Iterable[_INN_TV]], f: Callable[[_INN_TV], Any]) -> Optional[tuple]:
    if value is None:
        return None
    return tuple(map(f, value))


class ArrayIntTypeCaster(TypeCaster):
    cast_func = partial(_cast_array, f=IntegerTypeCaster.cast_func)


class ArrayFloatTypeCaster(TypeCaster):
    cast_func = partial(_cast_array, f=FloatTypeCaster.cast_func)


class ArrayStrTypeCaster(TypeCaster):
    cast_func = partial(_cast_array, f=StringTypeCaster.cast_func)


def _make_lowercase(value: Optional[str]) -> Optional[str]:
    if not value:
        return value
    return value.lower()


class LowercaseTypeCaster(TypeCaster):
    cast_func = _make_lowercase


class TypeTransformer:
    conn_type: ClassVar[ConnectionType]
    native_to_user_map: ClassVar[dict[GenericNativeType, BIType]] = {}
    user_to_native_map: ClassVar[dict[BIType, GenericNativeType]] = {}
    casters: ClassVar[dict[BIType, TypeCaster]] = {
        BIType.integer: IntegerTypeCaster(),
        BIType.float: FloatTypeCaster(),
        BIType.boolean: BooleanTypeCaster(),
        BIType.string: StringTypeCaster(),
        BIType.date: DateTypeCaster(),
        BIType.datetime: DatetimeTypeCaster(),
        BIType.datetimetz: DatetimeTZTypeCaster(),
        BIType.genericdatetime: GenericDatetimeTypeCaster(),
        BIType.geopoint: StringTypeCaster(),
        BIType.geopolygon: StringTypeCaster(),
        BIType.uuid: StringTypeCaster(),
        BIType.markup: StringTypeCaster(),
        BIType.array_int: ArrayIntTypeCaster(),
        BIType.array_float: ArrayFloatTypeCaster(),
        BIType.array_str: ArrayStrTypeCaster(),
        BIType.tree_str: ArrayStrTypeCaster(),  # Same as array
        BIType.unsupported: UnsupportedCaster(),
    }

    def type_native_to_user(
        self,
        native_t: GenericNativeType,
        user_t: Optional[BIType] = None,
    ) -> BIType:
        if user_t is not None:
            # original UT is given, try to validate against NT.
            # read as 'native type might have been made from the provided user type'.
            # Sample case: native 'uint8' might be a 'boolean' user type.
            if native_t.as_generic == self.user_to_native_map[user_t].as_generic:
                return user_t

        try:
            return self.native_to_user_map.get(native_t) or self.native_to_user_map[native_t.as_generic]
        except KeyError:
            raise exc.UnsupportedNativeTypeError(native_t)

    def make_foreign_native_type_conversion(self, native_t: GenericNativeType) -> GenericNativeType:
        """
        Attempt to make a native type for the current database that corresponds
        to a native type of another database.

        If there is no known direct conversion, should return `native_t` as-is
        (with non-matching `conn_type`).
        """
        return native_t  # no known conversion

    def type_user_to_native(self, user_t: BIType, native_t: Optional[GenericNativeType] = None) -> GenericNativeType:
        if native_t is not None:
            # original NT is given, try to do a direct conversion

            if native_t.conn_type != self.conn_type:
                # attempt to translate to own native type
                native_t = self.make_foreign_native_type_conversion(native_t)

            if native_t.conn_type == self.conn_type:
                # it is from the same DB type, so try to validate against UT
                if user_t == self.native_to_user_map.get(native_t) or user_t == self.native_to_user_map.get(
                    native_t.as_generic
                ):
                    return native_t

        try:
            result = self.user_to_native_map[user_t]
        except KeyError:
            raise exc.UnsupportedBITypeError(user_t)

        # Assume all databases support `nullable` in a similar way, so pass it
        # along if possible.
        if native_t is not None and isinstance(native_t, CommonNativeType):
            result = result.as_common().clone(nullable=native_t.nullable)

        return result

    @classmethod
    def cast_for_input(cls, value: Any, user_t: BIType) -> Any:
        """Prepare value for insertion into the database"""
        return cls.casters[user_t].cast_for_input(value=value)

    @classmethod
    def cast_for_output(cls, value: Any, user_t: Optional[BIType] = None) -> Any:
        """Convert value from DB to Python value conforming to given ``user_t``"""
        if user_t is None:
            return value
        return cls.casters[user_t].cast_for_output(value=value)


TYPE_TRANSFORMER_MAP: Dict[ConnectionType, TypeTransformer] = {}


def register_type_transformer_class(conn_type: ConnectionType, tt_cls: Type[TypeTransformer]) -> None:
    TYPE_TRANSFORMER_MAP[conn_type] = tt_cls()


@lru_cache(maxsize=32)
def get_type_transformer(conn_type: ConnectionType) -> TypeTransformer:
    try:
        return TYPE_TRANSFORMER_MAP[conn_type]
    except KeyError:
        raise exc.UnsupportedDatabaseError(conn_type)
