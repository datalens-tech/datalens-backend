from bi_connector_mysql.bi.connector import MySQLBiApiConnectionDefinition, MySQLBiApiConnector
from bi_connector_mysql_mdb.bi.api_schema.connection import MySQLMDBConnectionSchema
from bi_connector_mysql_mdb.bi.connection_form.form_config import MySQLMDBConnectionFormFactory


class MySQLMDBBiApiConnectionDefinition(MySQLBiApiConnectionDefinition):
    api_generic_schema_cls = MySQLMDBConnectionSchema
    form_factory_cls = MySQLMDBConnectionFormFactory


class MySQLMDBBiApiConnector(MySQLBiApiConnector):
    connection_definitions = (MySQLMDBBiApiConnectionDefinition,)
