from __future__ import annotations

import pytest

from dl_core.db.native_type import (
    ClickHouseDateTime64NativeType,
    ClickHouseDateTime64WithTZNativeType,
    ClickHouseDateTimeWithTZNativeType,
    ClickHouseNativeType,
    CommonNativeType,
    GenericNativeType,
    LengthedNativeType,
)
from dl_core.db.native_type_schema import OneOfNativeTypeSchema


SAMPLE_NATIVE_TYPES = (
    GenericNativeType(name="tinyblob"),
    CommonNativeType(name="double_precision", nullable=True),
    LengthedNativeType(name="nvarchar2", nullable=False, length=121),
    ClickHouseNativeType(name="uint64", nullable=True, lowcardinality=True),
    ClickHouseDateTimeWithTZNativeType(
        name="datetimewithtz",
        nullable=False,
        lowcardinality=True,
        timezone_name="Europe/Moscow",
    ),
    ClickHouseDateTime64NativeType(
        name="datetime64",
        nullable=False,
        lowcardinality=True,
        precision=3,
    ),
    ClickHouseDateTime64WithTZNativeType(
        name="datetime64withtz",
        nullable=False,
        lowcardinality=True,
        precision=3,
        timezone_name="Europe/Moscow",
    ),
)


@pytest.mark.parametrize(
    "native_type", SAMPLE_NATIVE_TYPES, ids=[obj.native_type_class_name for obj in SAMPLE_NATIVE_TYPES]
)
def test_native_type_schema(native_type):
    dumped = OneOfNativeTypeSchema().dump(native_type)
    assert dumped["native_type_class_name"] == native_type.native_type_class_name
    assert dumped["name"] == native_type.name

    loaded = OneOfNativeTypeSchema().load(dumped)
    assert type(loaded) is type(native_type)
    assert loaded == native_type
