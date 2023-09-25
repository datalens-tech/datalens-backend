from dl_connector_postgresql.api.connector import (
    PostgreSQLApiConnectionDefinition,
    PostgreSQLApiConnector,
)

from bi_connector_postgresql_mdb.bi.api_schema.connection import PostgreSQLMDBConnectionSchema
from bi_connector_postgresql_mdb.bi.connection_form.form_config import PostgreSQLMDBConnectionFormFactory
from bi_connector_postgresql_mdb.core.connector import PostgreSQLMDBCoreConnector


class PostgreSQLMDBApiConnectionDefinition(PostgreSQLApiConnectionDefinition):
    api_generic_schema_cls = PostgreSQLMDBConnectionSchema
    form_factory_cls = PostgreSQLMDBConnectionFormFactory


class PostgreSQLMDBApiConnector(PostgreSQLApiConnector):
    core_connector_cls = PostgreSQLMDBCoreConnector
    connection_definitions = (PostgreSQLMDBApiConnectionDefinition,)
