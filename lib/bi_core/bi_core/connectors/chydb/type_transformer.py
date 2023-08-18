from bi_constants.enums import ConnectionType as CT

from bi_core.connectors.clickhouse_base.type_transformer import ClickHouseTypeTransformer
from bi_core.db.conversion_base import make_native_type


class CHYDBTypeTransformer(ClickHouseTypeTransformer):
    conn_type = CT.chydb
    native_to_user_map = {
        make_native_type(CT.chydb, native_type.name): bi_type
        for native_type, bi_type in ClickHouseTypeTransformer.native_to_user_map.items()
    }
    user_to_native_map = {
        bi_type: make_native_type(CT.chydb, native_type.name)
        for bi_type, native_type in ClickHouseTypeTransformer.user_to_native_map.items()
    }
