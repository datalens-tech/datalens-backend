from __future__ import annotations

from typing import Sequence

import attr

from bi_api_lib_ya.connections_security.base import MDBDomainManager
from dl_connector_postgresql.core.postgresql_base.connection_executors import (
    AsyncPostgresConnExecutor,
    PostgresConnExecutor,
)
from dl_connector_postgresql.core.postgresql_base.constants import PGEnforceCollateMode

from bi_connector_mdb_base.core.connection_executors import MDBHostConnExecutorMixin


@attr.s(cmp=False, hash=False)
class PostgresMDBConnExecutor(MDBHostConnExecutorMixin, PostgresConnExecutor):
    pass


@attr.s(cmp=False, hash=False)
class AsyncPostgresMDBConnExecutor(MDBHostConnExecutorMixin, AsyncPostgresConnExecutor):
    def _get_effective_enforce_collate(
        self,
        enforce_collate: PGEnforceCollateMode,
        multihosts: Sequence[str],
    ) -> PGEnforceCollateMode:
        if enforce_collate == PGEnforceCollateMode.auto:
            db_domain_manager = self._sec_mgr.db_domain_manager
            assert isinstance(db_domain_manager, MDBDomainManager)
            is_mdb = all(db_domain_manager.host_in_mdb(host) for host in multihosts)
            enforce_collate = PGEnforceCollateMode.on if is_mdb else PGEnforceCollateMode.off
        return enforce_collate
