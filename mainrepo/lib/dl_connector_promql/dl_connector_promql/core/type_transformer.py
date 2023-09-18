from __future__ import annotations

from dl_connector_promql.core.constants import CONNECTION_TYPE_PROMQL
from dl_constants.enums import BIType
from dl_core.db.conversion_base import (
    TypeTransformer,
    make_native_type,
)


class PromQLTypeTransformer(TypeTransformer):
    conn_type = CONNECTION_TYPE_PROMQL
    native_to_user_map = {
        make_native_type(CONNECTION_TYPE_PROMQL, "unix_timestamp"): BIType.genericdatetime,
        make_native_type(CONNECTION_TYPE_PROMQL, "float64"): BIType.float,
        make_native_type(CONNECTION_TYPE_PROMQL, "string"): BIType.string,
    }
    user_to_native_map = dict(
        [(bi_type, native_type) for native_type, bi_type in native_to_user_map.items()]
        + [
            (BIType.datetime, make_native_type(CONNECTION_TYPE_PROMQL, "unix_timestamp")),
        ]
    )
