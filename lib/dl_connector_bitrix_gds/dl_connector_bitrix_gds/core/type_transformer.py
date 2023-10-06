from __future__ import annotations

from dl_constants.enums import UserDataType
from dl_core.db.conversion_base import (
    TypeTransformer,
    make_native_type,
)

from dl_connector_bitrix_gds.core.constants import CONNECTION_TYPE_BITRIX24


class BitrixGDSTypeTransformer(TypeTransformer):
    conn_type = CONNECTION_TYPE_BITRIX24
    native_to_user_map = {
        make_native_type(CONNECTION_TYPE_BITRIX24, "integer"): UserDataType.integer,
        make_native_type(CONNECTION_TYPE_BITRIX24, "float"): UserDataType.float,
        make_native_type(CONNECTION_TYPE_BITRIX24, "string"): UserDataType.string,
        make_native_type(CONNECTION_TYPE_BITRIX24, "date"): UserDataType.date,
        make_native_type(CONNECTION_TYPE_BITRIX24, "datetime"): UserDataType.genericdatetime,
    }
    user_to_native_map = dict(
        [(bi_type, native_type) for native_type, bi_type in native_to_user_map.items()]
        + [
            (UserDataType.datetime, make_native_type(CONNECTION_TYPE_BITRIX24, "datetime")),
        ]
    )
