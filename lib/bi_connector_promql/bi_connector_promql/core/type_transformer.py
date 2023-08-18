from __future__ import annotations

from bi_constants.enums import BIType, ConnectionType

from bi_core.db.conversion_base import TypeTransformer, make_native_type


class PromQLTypeTransformer(TypeTransformer):
    conn_type = ConnectionType.promql
    native_to_user_map = {
        make_native_type(ConnectionType.promql, 'unix_timestamp'): BIType.genericdatetime,
        make_native_type(ConnectionType.promql, 'float64'): BIType.float,
        make_native_type(ConnectionType.promql, 'string'): BIType.string,
    }
    user_to_native_map = dict([
        (bi_type, native_type) for native_type, bi_type in native_to_user_map.items()
    ] + [
        (BIType.datetime, make_native_type(ConnectionType.promql, 'unix_timestamp')),
    ])
