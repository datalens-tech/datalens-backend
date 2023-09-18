import enum
from typing import (
    ClassVar,
    Optional,
)

import attr

from bi_external_api.attrs_model_mapper import ModelDescriptor

from ...enums import ExtAPIType
from .common import Secret


class ConnectionKind(enum.Enum):
    clickhouse = "clickhouse"
    postgres = "postgres"
    ch_over_yt = "ch_over_yt"
    ch_over_yt_user_auth = "ch_over_yt_user_auth"


class RawSQLLevel(enum.Enum):
    off = "off"
    subselect = "subselect"
    dashsql = "dashsql"


@ModelDescriptor(is_abstract=True, children_type_discriminator_attr_name="kind")
@attr.s(frozen=True, kw_only=True)
class Connection:
    kind: ClassVar[ConnectionKind]

    raw_sql_level: RawSQLLevel = attr.ib()
    cache_ttl_sec: Optional[int] = attr.ib()


@ModelDescriptor(is_abstract=True)
@attr.s(frozen=True, kw_only=True)
class SQLConnection(Connection):
    host: str = attr.ib()
    port: int = attr.ib()

    username: str = attr.ib()


@ModelDescriptor()
@attr.s(frozen=True, kw_only=True)
class PostgresConnection(SQLConnection):
    kind = ConnectionKind.postgres

    database_name: str = attr.ib()


@ModelDescriptor()
@attr.s(frozen=True, kw_only=True)
class ClickHouseConnection(SQLConnection):
    kind = ConnectionKind.clickhouse

    secure: str = attr.ib()


@ModelDescriptor(is_abstract=True)
@attr.s(frozen=True, kw_only=True)
class BaseCHYTConnection(Connection):
    cluster: str = attr.ib()
    clique_alias: str = attr.ib()


@ModelDescriptor(api_types_exclude=[ExtAPIType.DC])
@attr.s(frozen=True, kw_only=True)
class CHYTConnection(BaseCHYTConnection):
    kind = ConnectionKind.ch_over_yt


@ModelDescriptor(api_types_exclude=[ExtAPIType.DC])
@attr.s(frozen=True, kw_only=True)
class CHYTUserAuthConnection(BaseCHYTConnection):
    kind = ConnectionKind.ch_over_yt_user_auth


@ModelDescriptor()
@attr.s(frozen=True, kw_only=True)
class ConnectionSecret:
    conn_name: str = attr.ib()
    secret: Secret = attr.ib(repr=False)
