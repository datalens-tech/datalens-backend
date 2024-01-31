from __future__ import annotations

from dl_connector_greenplum.core.us_connection import GreenplumConnection
from dl_connector_postgresql.api.api_schema.connection import PostgreSQLConnectionSchema


class GreenplumConnectionSchema(PostgreSQLConnectionSchema):
    TARGET_CLS = GreenplumConnection  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "type[GreenplumConnection]", base class "PostgreSQLConnectionSchema" defined the type as "type[ConnectionPostgreSQL]")  [assignment]
