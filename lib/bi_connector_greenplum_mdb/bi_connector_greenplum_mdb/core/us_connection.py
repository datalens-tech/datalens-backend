from __future__ import annotations

import attr

from bi_cloud_integration.mdb import MDBGreenplumClusterServiceClient
from dl_connector_greenplum.core.us_connection import GreenplumConnection

from bi_connector_mdb_base.core.base_models import ConnMDBDataModelMixin
from bi_connector_mdb_base.core.us_connection import MDBConnectionMixin


class GreenplumMDBConnection(MDBConnectionMixin, GreenplumConnection):
    MDB_CLIENT_CLS = MDBGreenplumClusterServiceClient

    @attr.s(kw_only=True)
    class DataModel(ConnMDBDataModelMixin, GreenplumConnection.DataModel):
        pass
