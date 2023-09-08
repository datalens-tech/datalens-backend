from bi_api_connector.api_schema.connection_mixins import MDBDatabaseSchemaMixin

from bi_connector_mysql.bi.api_schema.connection import MySQLConnectionSchema


class MySQLMDBConnectionSchema(MySQLConnectionSchema, MDBDatabaseSchemaMixin):
    pass
