from bi_api_connector.api_schema.connection_mixins import MDBDatabaseSchemaMixin

from bi_connector_postgresql.bi.api_schema.connection import PostgreSQLConnectionSchema


class PostgreSQLMDBConnectionSchema(PostgreSQLConnectionSchema, MDBDatabaseSchemaMixin):
    pass
