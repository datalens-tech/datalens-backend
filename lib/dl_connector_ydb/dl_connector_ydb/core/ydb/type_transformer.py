from __future__ import annotations

from dl_connector_ydb.core.base.type_transformer import YQLTypeTransformerBase
from dl_connector_ydb.core.ydb.constants import CONNECTION_TYPE_YDB


class YDBTypeTransformer(YQLTypeTransformerBase):
    conn_type = CONNECTION_TYPE_YDB

    native_to_user_map = {
        nt.clone(conn_type=CONNECTION_TYPE_YDB): bi_t for nt, bi_t in YQLTypeTransformerBase.native_to_user_map.items()
    }
    user_to_native_map = {
        bi_t: nt.clone(conn_type=CONNECTION_TYPE_YDB) for bi_t, nt in YQLTypeTransformerBase.user_to_native_map.items()
    }
