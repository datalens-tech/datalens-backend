from __future__ import annotations

from bi_constants.enums import BIType, ConnectionType

from bi_core.db.conversion_base import TypeTransformer, make_native_type


class BitrixGDSTypeTransformer(TypeTransformer):
    conn_type = ConnectionType.bitrix24
    native_to_user_map = {
        make_native_type(ConnectionType.bitrix24, 'integer'): BIType.integer,
        make_native_type(ConnectionType.bitrix24, 'float'): BIType.float,
        make_native_type(ConnectionType.bitrix24, 'string'): BIType.string,
        make_native_type(ConnectionType.bitrix24, 'date'): BIType.date,
        make_native_type(ConnectionType.bitrix24, 'datetime'): BIType.genericdatetime,
    }
    user_to_native_map = dict([
        (bi_type, native_type) for native_type, bi_type in native_to_user_map.items()
    ] + [
        (BIType.datetime, make_native_type(ConnectionType.bitrix24, 'datetime')),
    ])
