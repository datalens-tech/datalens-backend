from __future__ import annotations

from bi_connector_yql.core.yq.constants import CONNECTION_TYPE_YQ
from bi_connector_yql.core.yql_base.type_transformer import YQLTypeTransformerBase


class YQTypeTransformer(YQLTypeTransformerBase):
    conn_type = CONNECTION_TYPE_YQ

    native_to_user_map = {
        nt.clone(conn_type=CONNECTION_TYPE_YQ): bi_t for nt, bi_t in YQLTypeTransformerBase.native_to_user_map.items()
    }
    user_to_native_map = {
        bi_t: nt.clone(conn_type=CONNECTION_TYPE_YQ) for bi_t, nt in YQLTypeTransformerBase.user_to_native_map.items()
    }
