from bi_connector_mdb_base.bi.api_schema.connection_mixins import MDBDatabaseSchemaMixin
from bi_connector_mysql.bi.api_schema.connection import MySQLConnectionSchema
from bi_connector_mysql_mdb.core.us_connection import ConnectionMySQLMDB


class MySQLMDBConnectionSchema(MDBDatabaseSchemaMixin, MySQLConnectionSchema):
    TARGET_CLS = ConnectionMySQLMDB
