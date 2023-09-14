from __future__ import annotations

import attr

from bi_connector_mdb_base.core.us_connection import MDBConnectionMixin
from bi_connector_mdb_base.core.base_models import ConnMDBDataModelMixin
from bi_connector_mysql.core.us_connection import ConnectionMySQL
from bi_cloud_integration.mdb import MDBMySQLClusterServiceClient


class ConnectionMySQLMDB(MDBConnectionMixin, ConnectionMySQL):
    MDB_CLIENT_CLS = MDBMySQLClusterServiceClient

    @attr.s(kw_only=True)
    class DataModel(ConnMDBDataModelMixin, ConnectionMySQL.DataModel):
        pass
