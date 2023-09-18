from typing import ClassVar

from dl_constants.enums import BIType
from dl_constants.enums import ConnectionType as CT
from dl_core.db.conversion_base import (
    TypeCaster,
    YTBooleanTypeCaster,
    make_native_type,
)
from dl_core.db.native_type import GenericNativeType
from dl_sqlalchemy_chyt import types as chyt_types

from dl_connector_chyt.core.constants import CONNECTION_TYPE_CHYT
from dl_connector_clickhouse.core.clickhouse_base.type_transformer import ClickHouseTypeTransformer


def make_chyt_native_to_user_map(conn_type: CT) -> dict[GenericNativeType, BIType]:
    return {
        **{
            make_native_type(conn_type, native_type.name): bi_type
            for native_type, bi_type in ClickHouseTypeTransformer.native_to_user_map.items()
        },
        make_native_type(conn_type, chyt_types.YtBoolean): BIType.boolean,
    }


def make_chyt_user_to_native_map(conn_type: CT) -> dict[BIType, GenericNativeType]:
    return {
        **{
            bi_type: make_native_type(conn_type, native_type.name)
            for bi_type, native_type in ClickHouseTypeTransformer.user_to_native_map.items()
        },
        BIType.boolean: make_native_type(conn_type, chyt_types.YtBoolean),
    }


class CHYTTypeTransformer(ClickHouseTypeTransformer):
    conn_type = CONNECTION_TYPE_CHYT

    native_to_user_map = make_chyt_native_to_user_map(CONNECTION_TYPE_CHYT)
    user_to_native_map = make_chyt_user_to_native_map(CONNECTION_TYPE_CHYT)

    casters: ClassVar[dict[BIType, TypeCaster]] = {
        **ClickHouseTypeTransformer.casters,
        BIType.boolean: YTBooleanTypeCaster(),
    }
