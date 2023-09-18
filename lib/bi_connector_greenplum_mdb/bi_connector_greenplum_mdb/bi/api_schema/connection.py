from __future__ import annotations

from bi_connector_mdb_base.bi.api_schema.connection_mixins import MDBDatabaseSchemaMixin
from dl_connector_greenplum.bi.api_schema.connection import GreenplumConnectionSchema
from bi_connector_greenplum_mdb.core.us_connection import GreenplumMDBConnection


class GreenplumMDBConnectionSchema(MDBDatabaseSchemaMixin, GreenplumConnectionSchema):
    TARGET_CLS = GreenplumMDBConnection  # type: ignore
