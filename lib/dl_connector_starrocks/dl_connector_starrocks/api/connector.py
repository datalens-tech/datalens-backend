from dl_api_connector.api_schema.source_base import (
    SQLDataSourceSchema,
    SQLDataSourceTemplateSchema,
    SubselectDataSourceSchema,
    SubselectDataSourceTemplateSchema,
)
from dl_api_connector.connector import (
    ApiBackendDefinition,
    ApiConnectionDefinition,
    ApiConnector,
    ApiSourceDefinition,
)

from dl_connector_starrocks.api.api_schema.connection import StarRocksConnectionSchema
from dl_connector_starrocks.api.connection_form.form_config import StarRocksConnectionFormFactory
from dl_connector_starrocks.api.connection_info import StarRocksConnectionInfoProvider
from dl_connector_starrocks.api.i18n.localizer import CONFIGS
from dl_connector_starrocks.core.connector import (
    StarRocksCoreBackendDefinition,
    StarRocksCoreConnectionDefinition,
    StarRocksSubselectCoreSourceDefinition,
    StarRocksTableCoreSourceDefinition,
)
from dl_connector_starrocks.formula.constants import DIALECT_NAME_STARROCKS


class StarRocksApiTableSourceDefinition(ApiSourceDefinition):
    core_source_def_cls = StarRocksTableCoreSourceDefinition
    api_schema_cls = SQLDataSourceSchema
    template_api_schema_cls = SQLDataSourceTemplateSchema


class StarRocksApiSubselectSourceDefinition(ApiSourceDefinition):
    core_source_def_cls = StarRocksSubselectCoreSourceDefinition
    api_schema_cls = SubselectDataSourceSchema
    template_api_schema_cls = SubselectDataSourceTemplateSchema


class StarRocksApiConnectionDefinition(ApiConnectionDefinition):
    core_conn_def_cls = StarRocksCoreConnectionDefinition
    api_generic_schema_cls = StarRocksConnectionSchema
    info_provider_cls = StarRocksConnectionInfoProvider
    form_factory_cls = StarRocksConnectionFormFactory


class StarRocksApiBackendDefinition(ApiBackendDefinition):
    core_backend_definition = StarRocksCoreBackendDefinition
    formula_dialect_name = DIALECT_NAME_STARROCKS


class StarRocksApiConnector(ApiConnector):
    backend_definition = StarRocksApiBackendDefinition
    connection_definitions = (StarRocksApiConnectionDefinition,)
    source_definitions = (
        StarRocksApiTableSourceDefinition,
        StarRocksApiSubselectSourceDefinition,
    )
    translation_configs = frozenset(CONFIGS)
