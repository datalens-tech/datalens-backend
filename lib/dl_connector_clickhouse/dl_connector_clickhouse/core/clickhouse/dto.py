from __future__ import annotations

import attr

from dl_connector_clickhouse.core.clickhouse_base.dto import ClickHouseConnDTO


@attr.s(frozen=True)
class DLClickHouseConnDTO(ClickHouseConnDTO):
    readonly: int = attr.ib(kw_only=True, default=2)
