from __future__ import annotations

from bi_constants.enums import BIType, ConnectionType

from bi_core.db.conversion_base import TypeTransformer, make_native_type


class SolomonTypeTransformer(TypeTransformer):
    conn_type = ConnectionType.solomon
    native_to_user_map = {
        make_native_type(ConnectionType.solomon, 'datetime'): BIType.genericdatetime,
        make_native_type(ConnectionType.solomon, 'float'): BIType.float,
        make_native_type(ConnectionType.solomon, 'string'): BIType.string,
    }
    user_to_native_map = dict([
        (bi_type, native_type) for native_type, bi_type in native_to_user_map.items()
    ] + [
        (BIType.datetime, make_native_type(ConnectionType.solomon, 'datetime')),
    ])
