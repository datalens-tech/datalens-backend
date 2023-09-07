from bi_connector_greenplum.bi.connector import GreenplumBiApiConnectionDefinition, GreenplumBiApiConnector
from bi_connector_greenplum_mdb.bi.api_schema.connection import GreenplumMDBConnectionSchema
from bi_connector_greenplum_mdb.bi.connection_form.form_config import GreenplumMDBConnectionFormFactory


class GreenplumMDBBiApiConnectionDefinition(GreenplumBiApiConnectionDefinition):
    api_generic_schema_cls = GreenplumMDBConnectionSchema
    form_factory_cls = GreenplumMDBConnectionFormFactory


class GreenplumMDBBiApiConnector(GreenplumBiApiConnector):
    connection_definitions = (GreenplumMDBBiApiConnectionDefinition,)
