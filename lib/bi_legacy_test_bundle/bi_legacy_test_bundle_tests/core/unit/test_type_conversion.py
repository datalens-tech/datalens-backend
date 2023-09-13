from __future__ import annotations

import pytest

from bi_constants.enums import BIType
from bi_core.db import get_type_transformer
from bi_core.db.native_type import CommonNativeType

from bi_connector_clickhouse.core.clickhouse.constants import CONNECTION_TYPE_CLICKHOUSE
from bi_connector_postgresql.core.postgresql.constants import CONNECTION_TYPE_POSTGRES
from bi_connector_chyt_internal.core.constants import (
    CONNECTION_TYPE_CH_OVER_YT,
    CONNECTION_TYPE_CH_OVER_YT_USER_AUTH,
)


@pytest.mark.parametrize('connection_type', (
    CONNECTION_TYPE_CLICKHOUSE,
    CONNECTION_TYPE_CH_OVER_YT,
    CONNECTION_TYPE_CH_OVER_YT_USER_AUTH,
))
def test_foreign_conversion(connection_type):
    tt = get_type_transformer(connection_type)

    # matching types
    nt = CommonNativeType(conn_type=connection_type, name='uint64')
    mapped = tt.type_user_to_native(user_t=BIType.integer, native_t=nt)
    expected = CommonNativeType(conn_type=connection_type, name='uint64')
    assert mapped == expected

    nt = CommonNativeType(conn_type=connection_type, name='int16')
    mapped = tt.type_user_to_native(user_t=BIType.integer, native_t=nt)
    expected = CommonNativeType(conn_type=connection_type, name='int16')
    assert mapped == expected

    # non-matching types
    nt = CommonNativeType(conn_type=connection_type, name='uint64', nullable=False)
    mapped = tt.type_user_to_native(user_t=BIType.date, native_t=nt)
    # pass nullable, don't pass name (because user-type doesn't match)
    expected = CommonNativeType(conn_type=connection_type, name='date', nullable=False)
    assert mapped == expected


@pytest.mark.parametrize('array_type', (BIType.array_int, BIType.array_float, BIType.array_str))
def test_null_array_conversion(array_type):
    tt = get_type_transformer(CONNECTION_TYPE_POSTGRES)
    assert tt.cast_for_output(None, user_t=array_type) is None
