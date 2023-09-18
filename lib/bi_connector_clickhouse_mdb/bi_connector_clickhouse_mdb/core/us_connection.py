from __future__ import annotations

import attr

from bi_cloud_integration.mdb import MDBClickHouseClusterServiceClient
from dl_connector_clickhouse.core.clickhouse.us_connection import ConnectionClickhouse

from bi_connector_mdb_base.core.base_models import ConnMDBDataModelMixin
from bi_connector_mdb_base.core.us_connection import MDBConnectionMixin


class ConnectionClickhouseMDB(MDBConnectionMixin, ConnectionClickhouse):
    MDB_CLIENT_CLS = MDBClickHouseClusterServiceClient

    @attr.s(kw_only=True)
    class DataModel(ConnMDBDataModelMixin, ConnectionClickhouse.DataModel):
        pass
