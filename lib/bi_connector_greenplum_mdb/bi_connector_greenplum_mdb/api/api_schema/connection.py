from __future__ import annotations

from dl_connector_greenplum.api.api_schema.connection import GreenplumConnectionSchema

from bi_connector_greenplum_mdb.core.us_connection import GreenplumMDBConnection
from bi_connector_mdb_base.api.api_schema.connection_mixins import MDBDatabaseSchemaMixin


class GreenplumMDBConnectionSchema(MDBDatabaseSchemaMixin, GreenplumConnectionSchema):
    TARGET_CLS = GreenplumMDBConnection  # type: ignore
