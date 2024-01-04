from __future__ import annotations

from dl_constants.enums import UserDataType
from dl_core.db.conversion_base import (
    TypeTransformer,
    make_native_type,
)


class PromQLTypeTransformer(TypeTransformer):
    native_to_user_map = {
        make_native_type("unix_timestamp"): UserDataType.genericdatetime,
        make_native_type("float64"): UserDataType.float,
        make_native_type("string"): UserDataType.string,
    }
    user_to_native_map = dict(
        [(bi_type, native_type) for native_type, bi_type in native_to_user_map.items()]
        + [
            (UserDataType.datetime, make_native_type("unix_timestamp")),
        ]
    )
