from __future__ import annotations

from typing import Callable, Sequence, Optional

import attr

from bi_core.connection_executors.sync_base import SyncConnExecutorBase
from bi_core.us_connection_base import ConnectionBase, ClassicConnectionSQL
from bi_core.mdb_utils import MDBDomainManager

from bi_connector_postgresql.core.postgresql_base.constants import PGEnforceCollateMode


class ConnectionPostgreSQLBase(ClassicConnectionSQL):
    has_schema = True
    default_schema_name = 'public'

    @attr.s(kw_only=True)
    class DataModel(ClassicConnectionSQL.DataModel):
        enforce_collate: PGEnforceCollateMode = attr.ib(default=PGEnforceCollateMode.auto)
        ssl_enable: bool = attr.ib(kw_only=True, default=False)
        ssl_ca: Optional[str] = attr.ib(kw_only=True, default=None)

    @staticmethod
    def _get_effective_enforce_collate(
        enforce_collate: PGEnforceCollateMode, multihosts: Sequence[str],
    ) -> PGEnforceCollateMode:
        if enforce_collate == PGEnforceCollateMode.auto:
            mdb_man = MDBDomainManager.from_env()
            is_mdb = any(mdb_man.host_in_mdb(host) for host in multihosts)
            enforce_collate = (
                PGEnforceCollateMode.on
                if is_mdb
                else PGEnforceCollateMode.off)
        return enforce_collate

    def get_parameter_combinations(
            self, conn_executor_factory: Callable[[ConnectionBase], SyncConnExecutorBase],
    ) -> list[dict]:
        if not self.db_name:
            return []

        assert self.has_schema
        return [
            dict(schema_name=tid.schema_name, table_name=tid.table_name)
            for tid in self.get_tables(conn_executor_factory=conn_executor_factory, schema_name=None)
        ]
