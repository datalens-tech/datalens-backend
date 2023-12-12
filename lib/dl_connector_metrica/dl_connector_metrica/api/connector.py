from dl_api_connector.api_schema.source_base import (
    SQLDataSourceSchema,
    SQLDataSourceTemplateSchema,
)
from dl_api_connector.connector import (
    ApiBackendDefinition,
    ApiConnectionDefinition,
    ApiConnector,
    ApiSourceDefinition,
)

from dl_connector_metrica.api.api_schema.connection import (
    ConnectionAppMetricaAPISchema,
    ConnectionMetrikaAPISchema,
)
from dl_connector_metrica.api.connection_form.form_config import (
    AppMetricaAPIConnectionFormFactory,
    MetricaAPIConnectionFormFactory,
)
from dl_connector_metrica.api.connection_info import (
    AppMetricaConnectionInfoProvider,
    MetricaConnectionInfoProvider,
)
from dl_connector_metrica.api.filter_compiler import MetricaApiFilterFormulaCompiler
from dl_connector_metrica.api.i18n.localizer import CONFIGS
from dl_connector_metrica.core.connector import (
    AppMetricaApiCoreBackendDefinition,
    AppMetricaApiCoreConnectionDefinition,
    AppMetricaApiCoreConnector,
    AppMetricaApiCoreSourceDefinition,
    MetricaApiCoreBackendDefinition,
    MetricaApiCoreConnectionDefinition,
    MetricaApiCoreConnector,
    MetricaApiCoreSourceDefinition,
)
from dl_connector_metrica.formula.constants import DIALECT_NAME_METRICAAPI


class MetricaApiFilteredApiTableSourceDefinition(ApiSourceDefinition):
    core_source_def_cls = MetricaApiCoreSourceDefinition
    api_schema_cls = SQLDataSourceSchema
    template_api_schema_cls = SQLDataSourceTemplateSchema


class MetricaApiApiConnectionDefinition(ApiConnectionDefinition):
    core_conn_def_cls = MetricaApiCoreConnectionDefinition
    api_generic_schema_cls = ConnectionMetrikaAPISchema
    alias = "metrica"
    info_provider_cls = MetricaConnectionInfoProvider
    form_factory_cls = MetricaAPIConnectionFormFactory


class MetricaApiApiBackendDefinition(ApiBackendDefinition):
    core_backend_definition = MetricaApiCoreBackendDefinition
    formula_dialect_name = DIALECT_NAME_METRICAAPI
    filter_formula_compiler_cls = MetricaApiFilterFormulaCompiler


class MetricaApiApiConnector(ApiConnector):
    backend_definition = MetricaApiApiBackendDefinition
    connection_definitions = (MetricaApiApiConnectionDefinition,)
    source_definitions = (MetricaApiFilteredApiTableSourceDefinition,)
    translation_configs = frozenset(CONFIGS)


class AppMetricaApiFilteredApiTableSourceDefinition(ApiSourceDefinition):
    core_source_def_cls = AppMetricaApiCoreSourceDefinition
    api_schema_cls = SQLDataSourceSchema
    template_api_schema_cls = SQLDataSourceTemplateSchema


class AppMetricaApiApiConnectionDefinition(ApiConnectionDefinition):
    core_conn_def_cls = AppMetricaApiCoreConnectionDefinition
    api_generic_schema_cls = ConnectionAppMetricaAPISchema
    alias = "appmetrica"
    info_provider_cls = AppMetricaConnectionInfoProvider
    form_factory_cls = AppMetricaAPIConnectionFormFactory


class AppMetricaApiApiBackendDefinition(ApiBackendDefinition):
    core_backend_definition = AppMetricaApiCoreBackendDefinition
    formula_dialect_name = DIALECT_NAME_METRICAAPI
    filter_formula_compiler_cls = MetricaApiFilterFormulaCompiler


class AppMetricaApiApiConnector(ApiConnector):
    backend_definition = AppMetricaApiApiBackendDefinition
    connection_definitions = (AppMetricaApiApiConnectionDefinition,)
    source_definitions = (AppMetricaApiFilteredApiTableSourceDefinition,)
    translation_configs = frozenset(CONFIGS)
