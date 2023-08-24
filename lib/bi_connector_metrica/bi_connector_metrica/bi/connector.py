from bi_connector_metrica.core.connector import (
    MetricaApiCoreConnector, AppMetricaApiCoreConnector,
    MetricaApiCoreConnectionDefinition, AppMetricaApiCoreConnectionDefinition,
    MetricaApiCoreSourceDefinition, AppMetricaApiCoreSourceDefinition,
)
from bi_formula.core.dialect import DialectName

from bi_api_connector.connector import (
    BiApiSourceDefinition,
    BiApiConnectionDefinition,
    BiApiConnector,
)

from bi_api_connector.api_schema.source import SQLDataSourceSchema, SQLDataSourceTemplateSchema
from bi_connector_metrica.bi.api_schema.connection import ConnectionMetrikaAPISchema, ConnectionAppMetricaAPISchema
from bi_connector_metrica.bi.connection_form.form_config import (
    MetricaAPIConnectionFormFactory, AppMetricaAPIConnectionFormFactory,
)
from bi_connector_metrica.bi.connection_info import MetricaConnectionInfoProvider, AppMetricaConnectionInfoProvider
from bi_connector_metrica.bi.filter_compiler import MetricaApiFilterFormulaCompiler
from bi_connector_metrica.bi.i18n.localizer import CONFIGS


class MetricaApiFilteredBiApiTableSourceDefinition(BiApiSourceDefinition):
    core_source_def_cls = MetricaApiCoreSourceDefinition
    api_schema_cls = SQLDataSourceSchema
    template_api_schema_cls = SQLDataSourceTemplateSchema


class MetricaApiBiApiConnectionDefinition(BiApiConnectionDefinition):
    core_conn_def_cls = MetricaApiCoreConnectionDefinition
    api_generic_schema_cls = ConnectionMetrikaAPISchema
    alias = 'metrica'
    info_provider_cls = MetricaConnectionInfoProvider
    form_factory_cls = MetricaAPIConnectionFormFactory


class MetricaApiBiApiConnector(BiApiConnector):
    core_connector_cls = MetricaApiCoreConnector
    connection_definitions = (MetricaApiBiApiConnectionDefinition,)
    source_definitions = (MetricaApiFilteredBiApiTableSourceDefinition,)
    filter_formula_compiler_cls = MetricaApiFilterFormulaCompiler
    formula_dialect_name = DialectName.METRIKAAPI
    translation_configs = frozenset(CONFIGS)


class AppMetricaApiFilteredBiApiTableSourceDefinition(BiApiSourceDefinition):
    core_source_def_cls = AppMetricaApiCoreSourceDefinition
    api_schema_cls = SQLDataSourceSchema
    template_api_schema_cls = SQLDataSourceTemplateSchema


class AppMetricaApiBiApiConnectionDefinition(BiApiConnectionDefinition):
    core_conn_def_cls = AppMetricaApiCoreConnectionDefinition
    api_generic_schema_cls = ConnectionAppMetricaAPISchema
    alias = 'appmetrica'
    info_provider_cls = AppMetricaConnectionInfoProvider
    form_factory_cls = AppMetricaAPIConnectionFormFactory


class AppMetricaApiBiApiConnector(BiApiConnector):
    core_connector_cls = AppMetricaApiCoreConnector
    connection_definitions = (AppMetricaApiBiApiConnectionDefinition,)
    source_definitions = (AppMetricaApiFilteredBiApiTableSourceDefinition,)
    filter_formula_compiler_cls = MetricaApiFilterFormulaCompiler
    formula_dialect_name = DialectName.METRIKAAPI
    translation_configs = frozenset(CONFIGS)
