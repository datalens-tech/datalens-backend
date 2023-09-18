from dl_connector_postgresql.bi.connector import (
    PostgreSQLBiApiConnectionDefinition,
    PostgreSQLBiApiConnector,
)

from bi_connector_postgresql_mdb.bi.api_schema.connection import PostgreSQLMDBConnectionSchema
from bi_connector_postgresql_mdb.bi.connection_form.form_config import PostgreSQLMDBConnectionFormFactory
from bi_connector_postgresql_mdb.core.connector import PostgreSQLMDBCoreConnector


class PostgreSQLMDBBiApiConnectionDefinition(PostgreSQLBiApiConnectionDefinition):
    api_generic_schema_cls = PostgreSQLMDBConnectionSchema
    form_factory_cls = PostgreSQLMDBConnectionFormFactory


class PostgreSQLMDBBiApiConnector(PostgreSQLBiApiConnector):
    core_connector_cls = PostgreSQLMDBCoreConnector
    connection_definitions = (PostgreSQLMDBBiApiConnectionDefinition,)
