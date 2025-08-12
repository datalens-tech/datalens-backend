from __future__ import annotations

from typing import (
    Any,
    ClassVar,
    Optional,
    Union,
)

import attr
from sqlalchemy.types import TypeEngine
from typing_extensions import Self


SATypeSpec = Union[type[TypeEngine], TypeEngine, str, None]


def norm_native_type(native_t: SATypeSpec) -> Optional[str]:
    """Normalize native type to internal name"""
    if native_t is None:
        return None

    if hasattr(native_t, "__visit_name__"):
        native_t_name = native_t.__visit_name__
    elif not isinstance(native_t, str):
        native_t_name = type(native_t).__name__
    else:
        native_t_name = native_t

    if hasattr(native_t, "item_type"):
        item_type_name = norm_native_type(native_t.item_type)
        native_t_name = f"{native_t_name}({item_type_name})"

    return native_t_name.lower()


@attr.s(frozen=True)
class GenericNativeType:
    """
    Basic NativeType information usable for type-conversion maps.
    """

    native_type_class_name: ClassVar[str] = "generic_native_type"
    name: str = attr.ib()

    @classmethod
    def normalize_name_and_create(cls, name: SATypeSpec) -> Self:
        normalized_name = norm_native_type(name)
        assert normalized_name is not None
        return cls(name=normalized_name)

    @property
    def as_generic(self) -> GenericNativeType:  # for subclasses
        return GenericNativeType(name=self.name)

    def as_common(self, default_nullable: bool = True) -> CommonNativeType:
        """
        Helper method that converts a GenericNativeType object to a
        CommonNativeType object with the specified `nullable` value; for
        CommonNativeType objects does nothing.
        """
        return CommonNativeType(name=self.name, nullable=default_nullable)

    def clone(self, **kwargs: Any) -> Self:
        return attr.evolve(self, **kwargs)


@attr.s(frozen=True)
class CommonNativeType(GenericNativeType):
    native_type_class_name = "common_native_type"
    nullable: bool = attr.ib(default=True)

    @classmethod
    def normalize_name_and_create(cls, name: SATypeSpec, nullable: bool = True) -> Self:
        normalized_name = norm_native_type(name)
        assert normalized_name is not None
        return cls(name=normalized_name, nullable=nullable)

    def as_common(self, default_nullable: bool = True) -> Self:
        return self


@attr.s(frozen=True)
class LengthedNativeType(CommonNativeType):
    native_type_class_name = "lengthed_native_type"
    length: Optional[int] = attr.ib(default=None)


@attr.s(frozen=True)
class ClickHouseNativeType(CommonNativeType):
    native_type_class_name = "clickhouse_native_type"
    lowcardinality: bool = attr.ib(default=False)


@attr.s(frozen=True)
class ClickHouseDateTime64NativeType(ClickHouseNativeType):  # noqa
    native_type_class_name = "clickhouse_datetime64_native_type"
    precision: int = attr.ib(kw_only=True)


@attr.s(frozen=True)
class ClickHouseDateTimeWithTZNativeType(ClickHouseNativeType):
    native_type_class_name = "clickhouse_datetimewithtz_native_type"
    # It is kind-of non-optional, but hard to do this after all the inheritance.
    timezone_name: str = attr.ib(default="UTC")
    # Whether the timezone was specified explicitly in the type,
    # or was inferred from the system timezone.
    explicit_timezone: bool = attr.ib(default=True)


@attr.s(frozen=True)
class ClickHouseDateTime64WithTZNativeType(ClickHouseDateTime64NativeType):
    native_type_class_name = "clickhouse_datetime64withtz_native_type"
    # It is kind-of non-optional, but hard to do this after all the inheritance.
    timezone_name: str = attr.ib(default="UTC")
    # Whether the timezone was specified explicitly in the type,
    # or was inferred from the system timezone.
    explicit_timezone: bool = attr.ib(default=True)
