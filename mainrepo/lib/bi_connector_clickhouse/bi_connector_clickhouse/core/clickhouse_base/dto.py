from __future__ import annotations

from typing import Optional

import attr

from bi_constants.enums import ConnectionType
from bi_core.connection_models.dto_defs import (
    ConnDTO,
    DefaultSQLDTO,
)


@attr.s(frozen=True)
class ClickHouseBaseDTO(ConnDTO):
    pass


@attr.s(frozen=True)
class ClickHouseConnDTO(ClickHouseBaseDTO, DefaultSQLDTO):  # noqa
    conn_type = ConnectionType.clickhouse
    secure: bool = attr.ib(kw_only=True, default=False)
    ssl_ca: Optional[str] = attr.ib(kw_only=True, default=None)

    protocol: str = attr.ib(kw_only=True)
    # TODO CONSIDER: Is really optional
    endpoint: Optional[str] = attr.ib(kw_only=True)
    cluster_name: str = attr.ib(kw_only=True)
