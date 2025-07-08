from dl_api_connector.api_schema.source_base import (
    SchematizedSQLDataSourceSchema,
    SchematizedSQLDataSourceTemplateSchema,
    SubselectDataSourceSchema,
    SubselectDataSourceTemplateSchema,
)
from dl_api_connector.connector import (
    ApiBackendDefinition,
    ApiConnectionDefinition,
    ApiConnector,
    ApiSourceDefinition,
    MQMFactorySettingItem,
)
from dl_constants.enums import QueryProcessingMode
from dl_query_processing.multi_query.factory import NoCompengMultiQueryMutatorFactory

from dl_connector_trino.api.api_schema.connection import (
    TrinoConnectionSchema,
    TrinoConnectionSchemaBase,
)
from dl_connector_trino.api.connection_form.form_config import TrinoConnectionFormFactory
from dl_connector_trino.api.connection_info import TrinoConnectionInfoProvider
from dl_connector_trino.api.i18n.localizer import CONFIGS
from dl_connector_trino.core.connector import (
    TrinoCoreBackendDefinition,
    TrinoCoreConnectionDefinition,
    TrinoCoreSubselectSourceDefinition,
    TrinoCoreTableSourceDefinition,
)
from dl_connector_trino.formula.constants import (
    DIALECT_NAME_TRINO,
    TrinoDialect,
)


class TrinoApiTableSourceDefinition(ApiSourceDefinition):
    core_source_def_cls = TrinoCoreTableSourceDefinition
    api_schema_cls = SchematizedSQLDataSourceSchema
    template_api_schema_cls = SchematizedSQLDataSourceTemplateSchema


class TrinoApiSubselectSourceDefinition(ApiSourceDefinition):
    core_source_def_cls = TrinoCoreSubselectSourceDefinition
    api_schema_cls = SubselectDataSourceSchema
    template_api_schema_cls = SubselectDataSourceTemplateSchema


class TrinoApiConnectionDefinitionBase(ApiConnectionDefinition):
    core_conn_def_cls = TrinoCoreConnectionDefinition
    api_generic_schema_cls = TrinoConnectionSchemaBase
    info_provider_cls = TrinoConnectionInfoProvider
    form_factory_cls = TrinoConnectionFormFactory


class TrinoApiConnectionDefinition(TrinoApiConnectionDefinitionBase):
    api_generic_schema_cls = TrinoConnectionSchema


class TrinoApiBackendDefinition(ApiBackendDefinition):
    core_backend_definition = TrinoCoreBackendDefinition
    formula_dialect_name = DIALECT_NAME_TRINO
    multi_query_mutation_factories = ApiBackendDefinition.multi_query_mutation_factories + (
        MQMFactorySettingItem(
            query_proc_mode=QueryProcessingMode.native_wf,
            dialects=TrinoDialect.and_above(TrinoDialect.TRINO).to_list(),
            factory_cls=NoCompengMultiQueryMutatorFactory,
        ),
    )


class TrinoApiConnectorBase(ApiConnector):
    backend_definition = TrinoApiBackendDefinition
    connection_definitions = (TrinoApiConnectionDefinitionBase,)
    source_definitions = (
        TrinoApiTableSourceDefinition,
        TrinoApiSubselectSourceDefinition,
    )
    translation_configs = frozenset(CONFIGS)


class TrinoApiConnector(TrinoApiConnectorBase):
    connection_definitions = (TrinoApiConnectionDefinition,)
