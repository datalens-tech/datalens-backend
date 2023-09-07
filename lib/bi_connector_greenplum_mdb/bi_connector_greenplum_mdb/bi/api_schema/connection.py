from __future__ import annotations

from bi_connector_greenplum.core.us_connection import GreenplumConnection

from bi_connector_postgresql_mdb.bi.api_schema.connection import PostgreSQLMDBConnectionSchema


class GreenplumMDBConnectionSchema(PostgreSQLMDBConnectionSchema):
    TARGET_CLS = GreenplumConnection  # type: ignore
