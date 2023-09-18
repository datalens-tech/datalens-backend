from bi_connector_mdb_base.bi.api_schema.connection_mixins import MDBDatabaseSchemaMixin
from bi_connector_mysql.bi.api_schema.connection import MySQLConnectionSchema


class MySQLMDBConnectionSchema(MDBDatabaseSchemaMixin, MySQLConnectionSchema):
    pass
