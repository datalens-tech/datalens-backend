from dl_api_connector.api_schema.source_base import (
    SubselectDataSourceSchema,
    SubselectDataSourceTemplateSchema,
)
from dl_api_connector.connector import (
    ApiBackendDefinition,
    ApiConnectionDefinition,
    ApiConnector,
    ApiSourceDefinition,
)

from dl_connector_bigquery.api.api_schema.connection import BigQueryConnectionSchema
from dl_connector_bigquery.api.api_schema.source import (
    BigQueryTableDataSourceSchema,
    BigQueryTableDataSourceTemplateSchema,
)
from dl_connector_bigquery.api.connection_form.form_config import BigQueryConnectionFormFactory
from dl_connector_bigquery.api.connection_info import BigQueryConnectionInfoProvider
from dl_connector_bigquery.api.i18n.localizer import CONFIGS
from dl_connector_bigquery.core.connector import (
    BigQueryCoreBackendDefinition,
    BigQueryCoreConnectionDefinition,
    BigQueryCoreSubselectSourceDefinition,
    BigQueryCoreTableSourceDefinition,
)
from dl_connector_bigquery.formula.constants import DIALECT_NAME_BIGQUERY


class BigQueryApiTableSourceDefinition(ApiSourceDefinition):
    core_source_def_cls = BigQueryCoreTableSourceDefinition
    api_schema_cls = BigQueryTableDataSourceSchema
    template_api_schema_cls = BigQueryTableDataSourceTemplateSchema


class BigQueryApiSubselectSourceDefinition(ApiSourceDefinition):
    core_source_def_cls = BigQueryCoreSubselectSourceDefinition
    api_schema_cls = SubselectDataSourceSchema
    template_api_schema_cls = SubselectDataSourceTemplateSchema


class BigQueryApiConnectionDefinition(ApiConnectionDefinition):
    core_conn_def_cls = BigQueryCoreConnectionDefinition
    api_generic_schema_cls = BigQueryConnectionSchema
    info_provider_cls = BigQueryConnectionInfoProvider
    form_factory_cls = BigQueryConnectionFormFactory


class BigQueryApiBackendDefinition(ApiBackendDefinition):
    core_backend_definition = BigQueryCoreBackendDefinition
    formula_dialect_name = DIALECT_NAME_BIGQUERY


class BigQueryApiConnector(ApiConnector):
    backend_definition = BigQueryApiBackendDefinition
    source_definitions = (
        BigQueryApiTableSourceDefinition,
        BigQueryApiSubselectSourceDefinition,
    )
    connection_definitions = (BigQueryApiConnectionDefinition,)
    translation_configs = frozenset(CONFIGS)
