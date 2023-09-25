from bi_connector_mysql.bi.connector import (
    MySQLApiConnectionDefinition,
    MySQLApiConnector,
)
from bi_connector_mysql_mdb.bi.api_schema.connection import MySQLMDBConnectionSchema
from bi_connector_mysql_mdb.bi.connection_form.form_config import MySQLMDBConnectionFormFactory
from bi_connector_mysql_mdb.core.connector import MySQLMDBCoreConnector


class MySQLMDBApiConnectionDefinition(MySQLApiConnectionDefinition):
    api_generic_schema_cls = MySQLMDBConnectionSchema
    form_factory_cls = MySQLMDBConnectionFormFactory


class MySQLMDBApiConnector(MySQLApiConnector):
    core_connector_cls = MySQLMDBCoreConnector
    connection_definitions = (MySQLMDBApiConnectionDefinition,)
