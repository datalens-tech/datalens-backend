from __future__ import annotations

from typing import Optional

import attr

from bi_constants.enums import ConnectionType

from bi_core.connectors.clickhouse_base.dto import ClickHouseBaseDTO


@attr.s(frozen=True)
class CHOverYDBDTO(ClickHouseBaseDTO):  # noqa
    conn_type = ConnectionType.chydb

    protocol: str = attr.ib(kw_only=True)
    host: str = attr.ib(kw_only=True)
    port: int = attr.ib(kw_only=True)
    endpoint: Optional[str] = attr.ib(kw_only=True)
    token: str = attr.ib(repr=False, kw_only=True)

    def get_all_hosts(self) -> list[str]:
        return [self.host] if self.host else []
