from bi_connector_greenplum.core.connector import (
    GreenplumCoreConnector,
    GreenplumCoreConnectionDefinition,
    GreenplumTableCoreSourceDefinition,
    GreenplumSubselectCoreSourceDefinition,
)

from bi_api_connector.connector import (
    BiApiSourceDefinition,
    BiApiConnectionDefinition,
    BiApiConnector,
)

from bi_api_connector.api_schema.source_base import (
    SchematizedSQLDataSourceSchema, SchematizedSQLDataSourceTemplateSchema,
    SubselectDataSourceSchema, SubselectDataSourceTemplateSchema,
)

from bi_connector_greenplum.bi.api_schema.connection import GreenplumConnectionSchema
from bi_connector_greenplum.bi.connection_form.form_config import GreenplumConnectionFormFactory
from bi_connector_greenplum.bi.connection_info import GreenplumConnectionInfoProvider
from bi_connector_greenplum.bi.i18n.localizer import CONFIGS
from bi_connector_postgresql.formula.constants import DIALECT_NAME_POSTGRESQL


class GreenplumBiApiTableSourceDefinition(BiApiSourceDefinition):
    core_source_def_cls = GreenplumTableCoreSourceDefinition
    api_schema_cls = SchematizedSQLDataSourceSchema
    template_api_schema_cls = SchematizedSQLDataSourceTemplateSchema


class GreenplumBiApiSubselectSourceDefinition(BiApiSourceDefinition):
    core_source_def_cls = GreenplumSubselectCoreSourceDefinition
    api_schema_cls = SubselectDataSourceSchema
    template_api_schema_cls = SubselectDataSourceTemplateSchema


class GreenplumBiApiConnectionDefinition(BiApiConnectionDefinition):
    core_conn_def_cls = GreenplumCoreConnectionDefinition
    api_generic_schema_cls = GreenplumConnectionSchema
    info_provider_cls = GreenplumConnectionInfoProvider
    form_factory_cls = GreenplumConnectionFormFactory


class GreenplumBiApiConnector(BiApiConnector):
    core_connector_cls = GreenplumCoreConnector
    connection_definitions = (GreenplumBiApiConnectionDefinition,)
    source_definitions = (
        GreenplumBiApiTableSourceDefinition,
        GreenplumBiApiSubselectSourceDefinition,
    )
    formula_dialect_name = DIALECT_NAME_POSTGRESQL
    translation_configs = frozenset(CONFIGS)
