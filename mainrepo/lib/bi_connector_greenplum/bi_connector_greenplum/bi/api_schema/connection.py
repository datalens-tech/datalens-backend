from __future__ import annotations

from bi_connector_greenplum.core.us_connection import GreenplumConnection

from bi_connector_postgresql.bi.api_schema.connection import PostgreSQLConnectionSchema


class GreenplumConnectionSchema(PostgreSQLConnectionSchema):
    TARGET_CLS = GreenplumConnection  # type: ignore
