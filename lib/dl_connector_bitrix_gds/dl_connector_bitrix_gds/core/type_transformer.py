from __future__ import annotations

from dl_constants.enums import UserDataType
from dl_type_transformer.type_transformer import (
    TypeTransformer,
    make_native_type,
)


class BitrixGDSTypeTransformer(TypeTransformer):
    native_to_user_map = {
        make_native_type("integer"): UserDataType.integer,
        make_native_type("float"): UserDataType.float,
        make_native_type("string"): UserDataType.string,
        make_native_type("date"): UserDataType.date,
        make_native_type("datetime"): UserDataType.genericdatetime,
    }
    user_to_native_map = dict(
        [(bi_type, native_type) for native_type, bi_type in native_to_user_map.items()]
        + [
            (UserDataType.datetime, make_native_type("datetime")),
        ]
    )
