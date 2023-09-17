from __future__ import annotations

import pytest

from bi_core.db.native_type import (
    GenericNativeType, CommonNativeType, LengthedNativeType,
    ClickHouseNativeType, ClickHouseDateTimeWithTZNativeType,
    ClickHouseDateTime64NativeType, ClickHouseDateTime64WithTZNativeType,
)
from bi_core.db.native_type_schema import OneOfNativeTypeSchema

from bi_connector_clickhouse.core.clickhouse_base.constants import CONNECTION_TYPE_CLICKHOUSE
from bi_connector_mysql.core.constants import CONNECTION_TYPE_MYSQL
from bi_connector_oracle.core.constants import CONNECTION_TYPE_ORACLE
from bi_connector_postgresql.core.postgresql.constants import CONNECTION_TYPE_POSTGRES


SAMPLE_NATIVE_TYPES = (
    GenericNativeType(
        conn_type=CONNECTION_TYPE_MYSQL,
        name='tinyblob'),
    CommonNativeType(
        conn_type=CONNECTION_TYPE_POSTGRES,
        name='double_precision',
        nullable=True),
    LengthedNativeType(
        conn_type=CONNECTION_TYPE_ORACLE,
        name='nvarchar2',
        nullable=False,
        length=121),
    ClickHouseNativeType(
        conn_type=CONNECTION_TYPE_CLICKHOUSE,
        name='uint64',
        nullable=True,
        lowcardinality=True),
    ClickHouseDateTimeWithTZNativeType(
        conn_type=CONNECTION_TYPE_CLICKHOUSE,
        name='datetimewithtz',
        nullable=False,
        lowcardinality=True,
        timezone_name='Europe/Moscow',
    ),
    ClickHouseDateTime64NativeType(
        conn_type=CONNECTION_TYPE_CLICKHOUSE,
        name='datetime64',
        nullable=False,
        lowcardinality=True,
        precision=3,
    ),
    ClickHouseDateTime64WithTZNativeType(
        conn_type=CONNECTION_TYPE_CLICKHOUSE,
        name='datetime64withtz',
        nullable=False,
        lowcardinality=True,
        precision=3,
        timezone_name='Europe/Moscow',
    ),
)


@pytest.fixture(
    params=SAMPLE_NATIVE_TYPES,
    ids=[obj.native_type_class_name for obj in SAMPLE_NATIVE_TYPES])
def some_native_type(request):
    return request.param


def test_native_type_schema(some_native_type):
    dumped = OneOfNativeTypeSchema().dump(some_native_type)
    if isinstance(dumped, str):  # Transition
        return
    assert dumped['native_type_class_name'] == some_native_type.native_type_class_name
    assert dumped['conn_type'] == some_native_type.conn_type.value
    assert dumped['name'] == some_native_type.name

    loaded = OneOfNativeTypeSchema().load(dumped)
    if isinstance(loaded, str):
        return  # Transition-state, the test isn't valid. TODO: remove.
    assert type(loaded) is type(some_native_type)
    assert loaded == some_native_type, 'roundtrip'
