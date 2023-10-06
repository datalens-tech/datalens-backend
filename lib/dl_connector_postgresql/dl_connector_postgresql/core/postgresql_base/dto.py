from __future__ import annotations

import typing

import attr

from dl_core.connection_models.dto_defs import DefaultSQLDTO

from dl_connector_postgresql.core.postgresql_base.constants import PGEnforceCollateMode


@attr.s(frozen=True)
class PostgresConnDTOBase(DefaultSQLDTO):
    enforce_collate: PGEnforceCollateMode = attr.ib(kw_only=True, default=PGEnforceCollateMode.off)
    ssl_enable: bool = attr.ib(kw_only=True, default=False)
    ssl_ca: typing.Optional[str] = attr.ib(kw_only=True, default=None)
