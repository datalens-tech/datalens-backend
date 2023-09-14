from bi_connector_mysql.bi.connector import MySQLBiApiConnectionDefinition, MySQLBiApiConnector
from bi_connector_mysql_mdb.bi.api_schema.connection import MySQLMDBConnectionSchema
from bi_connector_mysql_mdb.bi.connection_form.form_config import MySQLMDBConnectionFormFactory
from bi_connector_mysql_mdb.core.connector import MySQLMDBCoreConnector


class MySQLMDBBiApiConnectionDefinition(MySQLBiApiConnectionDefinition):
    api_generic_schema_cls = MySQLMDBConnectionSchema
    form_factory_cls = MySQLMDBConnectionFormFactory


class MySQLMDBBiApiConnector(MySQLBiApiConnector):
    core_connector_cls = MySQLMDBCoreConnector
    connection_definitions = (MySQLMDBBiApiConnectionDefinition,)
