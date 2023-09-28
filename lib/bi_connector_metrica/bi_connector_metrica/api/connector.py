from dl_api_connector.api_schema.source_base import (
    SQLDataSourceSchema,
    SQLDataSourceTemplateSchema,
)
from dl_api_connector.connector import (
    ApiConnectionDefinition,
    ApiConnector,
    ApiSourceDefinition,
)

from bi_connector_metrica.api.api_schema.connection import (
    ConnectionAppMetricaAPISchema,
    ConnectionMetrikaAPISchema,
)
from bi_connector_metrica.api.connection_form.form_config import (
    AppMetricaAPIConnectionFormFactory,
    MetricaAPIConnectionFormFactory,
)
from bi_connector_metrica.api.connection_info import (
    AppMetricaConnectionInfoProvider,
    MetricaConnectionInfoProvider,
)
from bi_connector_metrica.api.filter_compiler import MetricaApiFilterFormulaCompiler
from bi_connector_metrica.api.i18n.localizer import CONFIGS
from bi_connector_metrica.core.connector import (
    AppMetricaApiCoreConnectionDefinition,
    AppMetricaApiCoreConnector,
    AppMetricaApiCoreSourceDefinition,
    MetricaApiCoreConnectionDefinition,
    MetricaApiCoreConnector,
    MetricaApiCoreSourceDefinition,
)
from bi_connector_metrica.formula.constants import DIALECT_NAME_METRICAAPI


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


class MetricaApiApiConnector(ApiConnector):
    core_connector_cls = MetricaApiCoreConnector
    connection_definitions = (MetricaApiApiConnectionDefinition,)
    source_definitions = (MetricaApiFilteredApiTableSourceDefinition,)
    filter_formula_compiler_cls = MetricaApiFilterFormulaCompiler
    formula_dialect_name = DIALECT_NAME_METRICAAPI
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


class AppMetricaApiApiConnector(ApiConnector):
    core_connector_cls = AppMetricaApiCoreConnector
    connection_definitions = (AppMetricaApiApiConnectionDefinition,)
    source_definitions = (AppMetricaApiFilteredApiTableSourceDefinition,)
    filter_formula_compiler_cls = MetricaApiFilterFormulaCompiler
    formula_dialect_name = DIALECT_NAME_METRICAAPI
    translation_configs = frozenset(CONFIGS)
