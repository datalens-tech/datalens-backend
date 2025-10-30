from __future__ import annotations

from typing import (
    Callable,
    Optional,
)

import attr

from dl_core.connection_executors.sync_base import SyncConnExecutorBase
from dl_core.us_connection_base import (
    ClassicConnectionSQL,
    ConnectionBase,
)

from dl_connector_postgresql.core.postgresql_base.constants import PGEnforceCollateMode


class ConnectionPostgreSQLBase(ClassicConnectionSQL):
    has_schema = True
    default_schema_name = "public"
    supports_source_search = True
    supports_source_pagination = True

    @attr.s(kw_only=True)
    class DataModel(ClassicConnectionSQL.DataModel):
        enforce_collate: PGEnforceCollateMode = attr.ib(default=PGEnforceCollateMode.auto)
        ssl_enable: bool = attr.ib(kw_only=True, default=False)
        ssl_ca: Optional[str] = attr.ib(kw_only=True, default=None)

    def get_parameter_combinations(
        self,
        conn_executor_factory: Callable[[ConnectionBase], SyncConnExecutorBase],
        search_text: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
        db_name: str | None = None,
    ) -> list[dict]:
        if not self.db_name:
            return []

        assert self.has_schema
        return [
            dict(schema_name=tid.schema_name, table_name=tid.table_name)
            for tid in self.get_tables(
                conn_executor_factory=conn_executor_factory,
                search_text=search_text,
                limit=limit,
                offset=offset
            )
        ]
