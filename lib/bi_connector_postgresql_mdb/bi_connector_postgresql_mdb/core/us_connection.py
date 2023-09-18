from __future__ import annotations

from typing import Sequence

import attr

from dl_core.mdb_utils import MDBDomainManager
from dl_connector_postgresql.core.postgresql_base.constants import PGEnforceCollateMode
from bi_connector_mdb_base.core.us_connection import MDBConnectionMixin
from bi_connector_mdb_base.core.base_models import ConnMDBDataModelMixin
from bi_cloud_integration.mdb import MDBPostgreSQLClusterServiceClient

from dl_connector_postgresql.core.postgresql.us_connection import ConnectionPostgreSQL


class ConnectionPostgreSQLMDB(MDBConnectionMixin, ConnectionPostgreSQL):
    MDB_CLIENT_CLS = MDBPostgreSQLClusterServiceClient

    @attr.s(kw_only=True)
    class DataModel(ConnMDBDataModelMixin, ConnectionPostgreSQL.DataModel):
        pass

    @staticmethod
    def _get_effective_enforce_collate(
        enforce_collate: PGEnforceCollateMode,
        multihosts: Sequence[str],
    ) -> PGEnforceCollateMode:
        if enforce_collate == PGEnforceCollateMode.auto:
            mdb_man = MDBDomainManager.from_env()
            is_mdb = any(mdb_man.host_in_mdb(host) for host in multihosts)
            enforce_collate = PGEnforceCollateMode.on if is_mdb else PGEnforceCollateMode.off
        return enforce_collate
