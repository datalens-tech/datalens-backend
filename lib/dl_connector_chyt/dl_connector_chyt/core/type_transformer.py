from typing import ClassVar

from dl_constants.enums import UserDataType
from dl_core.db.conversion_base import (
    TypeCaster,
    YTBooleanTypeCaster,
    make_native_type,
)
from dl_core.db.native_type import GenericNativeType
from dl_sqlalchemy_chyt import types as chyt_types

from dl_connector_clickhouse.core.clickhouse_base.type_transformer import ClickHouseTypeTransformer


def make_chyt_native_to_user_map() -> dict[GenericNativeType, UserDataType]:
    return {
        **{
            make_native_type(native_type.name): bi_type
            for native_type, bi_type in ClickHouseTypeTransformer.native_to_user_map.items()
        },
        make_native_type(chyt_types.YtBoolean): UserDataType.boolean,
    }


def make_chyt_user_to_native_map() -> dict[UserDataType, GenericNativeType]:
    return {
        **{
            bi_type: make_native_type(native_type.name)
            for bi_type, native_type in ClickHouseTypeTransformer.user_to_native_map.items()
        },
        UserDataType.boolean: make_native_type(chyt_types.YtBoolean),
    }


class CHYTTypeTransformer(ClickHouseTypeTransformer):
    native_to_user_map = make_chyt_native_to_user_map()
    user_to_native_map = make_chyt_user_to_native_map()

    casters: ClassVar[dict[UserDataType, TypeCaster]] = {
        **ClickHouseTypeTransformer.casters,
        UserDataType.boolean: YTBooleanTypeCaster(),
    }
