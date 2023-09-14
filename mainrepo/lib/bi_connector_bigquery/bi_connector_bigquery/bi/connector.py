from bi_api_connector.api_schema.source_base import (
    SubselectDataSourceSchema,
    SubselectDataSourceTemplateSchema,
)
from bi_api_connector.connector import (
    BiApiConnectionDefinition,
    BiApiConnector,
    BiApiSourceDefinition,
)

from bi_connector_bigquery.bi.api_schema.connection import BigQueryConnectionSchema
from bi_connector_bigquery.bi.api_schema.source import (
    BigQueryTableDataSourceSchema,
    BigQueryTableDataSourceTemplateSchema,
)
from bi_connector_bigquery.bi.connection_form.form_config import BigQueryConnectionFormFactory
from bi_connector_bigquery.bi.connection_info import BigQueryConnectionInfoProvider
from bi_connector_bigquery.bi.i18n.localizer import CONFIGS
from bi_connector_bigquery.core.connector import (
    BigQueryCoreConnectionDefinition,
    BigQueryCoreConnector,
    BigQueryCoreSubselectSourceDefinition,
    BigQueryCoreTableSourceDefinition,
)
from bi_connector_bigquery.formula.constants import DIALECT_NAME_BIGQUERY


class BigQueryBiApiTableSourceDefinition(BiApiSourceDefinition):
    core_source_def_cls = BigQueryCoreTableSourceDefinition
    api_schema_cls = BigQueryTableDataSourceSchema
    template_api_schema_cls = BigQueryTableDataSourceTemplateSchema


class BigQueryBiApiSubselectSourceDefinition(BiApiSourceDefinition):
    core_source_def_cls = BigQueryCoreSubselectSourceDefinition
    api_schema_cls = SubselectDataSourceSchema
    template_api_schema_cls = SubselectDataSourceTemplateSchema


class BigQueryBiApiConnectionDefinition(BiApiConnectionDefinition):
    core_conn_def_cls = BigQueryCoreConnectionDefinition
    api_generic_schema_cls = BigQueryConnectionSchema
    info_provider_cls = BigQueryConnectionInfoProvider
    form_factory_cls = BigQueryConnectionFormFactory


class BigQueryBiApiConnector(BiApiConnector):
    core_connector_cls = BigQueryCoreConnector
    source_definitions = (
        BigQueryBiApiTableSourceDefinition,
        BigQueryBiApiSubselectSourceDefinition,
    )
    connection_definitions = (BigQueryBiApiConnectionDefinition,)
    formula_dialect_name = DIALECT_NAME_BIGQUERY
    translation_configs = frozenset(CONFIGS)
