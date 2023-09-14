from __future__ import annotations

import attr

from bi_connector_mdb_base.core.us_connection import MDBConnectionMixin
from bi_connector_mdb_base.core.base_models import ConnMDBDataModelMixin
from bi_cloud_integration.mdb import MDBPostgreSQLClusterServiceClient

from bi_connector_postgresql.core.postgresql.us_connection import ConnectionPostgreSQL


class ConnectionPostgreSQLMDB(MDBConnectionMixin, ConnectionPostgreSQL):
    MDB_CLIENT_CLS = MDBPostgreSQLClusterServiceClient

    @attr.s(kw_only=True)
    class DataModel(ConnMDBDataModelMixin, ConnectionPostgreSQL.DataModel):
        pass
