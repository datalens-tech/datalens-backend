from __future__ import annotations

from typing import TYPE_CHECKING, Optional

import attr

from bi_constants.enums import BIType

from bi_api_commons.base_models import RequestContextInfo

from bi_core.data_processing.cache.primitives import CacheTTLConfig
from bi_core.db import SchemaColumn
from bi_core.connectors.clickhouse_base.type_transformer import ClickHouseTypeTransformer

if TYPE_CHECKING:
    from bi_core.db.conversion_base import TypeTransformer


def sc(name: str, user_type: BIType, nullable: bool = False, type_transformer: Optional[TypeTransformer] = None):  # type: ignore  # TODO: fix
    # FIXME: Refactor or remove this
    """
    Creates full schema column based on user type and nullable.
    :param name: Column name
    :param user_type: User type
    :param nullable: Is column nullable
    :param type_transformer: Type transformer to convert user type to native type. Default is ClickHouse transformer
    :return: Schema column
    """
    if type_transformer is None:
        type_transformer = ClickHouseTypeTransformer()

    return SchemaColumn(
        title=name,
        name=name,
        nullable=nullable,
        native_type=type_transformer.type_user_to_native(user_type),
        user_type=user_type,
    )


@attr.s(auto_attribs=True, frozen=True)
class SROptions:
    rci: RequestContextInfo
    with_caches: bool = False
    cache_save_background: Optional[bool] = False
    default_caches_ttl_config: CacheTTLConfig = CacheTTLConfig()
    with_compeng_pg: bool = False
