from dl_connector_greenplum.api.connector import (
    GreenplumApiConnectionDefinition,
    GreenplumApiConnector,
)

from bi_connector_greenplum_mdb.bi.api_schema.connection import GreenplumMDBConnectionSchema
from bi_connector_greenplum_mdb.bi.connection_form.form_config import GreenplumMDBConnectionFormFactory
from bi_connector_greenplum_mdb.core.connector import GreenplumMDBCoreConnector


class GreenplumMDBApiConnectionDefinition(GreenplumApiConnectionDefinition):
    api_generic_schema_cls = GreenplumMDBConnectionSchema
    form_factory_cls = GreenplumMDBConnectionFormFactory


class GreenplumMDBApiConnector(GreenplumApiConnector):
    core_connector_cls = GreenplumMDBCoreConnector
    connection_definitions = (GreenplumMDBApiConnectionDefinition,)
