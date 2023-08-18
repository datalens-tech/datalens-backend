from __future__ import annotations

from bi_connector_postgresql.core.greenplum.us_connection import GreenplumConnection

from bi_api_lib.connectors.postgresql.api_schema.connection import PostgreSQLConnectionSchema


class GreenplumConnectionSchema(PostgreSQLConnectionSchema):
    TARGET_CLS = GreenplumConnection  # type: ignore
