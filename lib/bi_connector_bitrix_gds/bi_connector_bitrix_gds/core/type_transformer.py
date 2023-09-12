from __future__ import annotations

from bi_constants.enums import BIType

from bi_core.db.conversion_base import TypeTransformer, make_native_type

from bi_connector_bitrix_gds.core.constants import CONNECTION_TYPE_BITRIX24


class BitrixGDSTypeTransformer(TypeTransformer):
    conn_type = CONNECTION_TYPE_BITRIX24
    native_to_user_map = {
        make_native_type(CONNECTION_TYPE_BITRIX24, 'integer'): BIType.integer,
        make_native_type(CONNECTION_TYPE_BITRIX24, 'float'): BIType.float,
        make_native_type(CONNECTION_TYPE_BITRIX24, 'string'): BIType.string,
        make_native_type(CONNECTION_TYPE_BITRIX24, 'date'): BIType.date,
        make_native_type(CONNECTION_TYPE_BITRIX24, 'datetime'): BIType.genericdatetime,
    }
    user_to_native_map = dict([
        (bi_type, native_type) for native_type, bi_type in native_to_user_map.items()
    ] + [
        (BIType.datetime, make_native_type(CONNECTION_TYPE_BITRIX24, 'datetime')),
    ])
