from bi_connector_bundle_ch_filtered_ya_cloud.ch_geo_filtered.core.connector import (
    ClickhouseGeoFilteredCoreConnector,
    ClickhouseGeoFilteredCoreConnectionDefinition,
    ClickhouseGeoFilteredTableCoreSourceDefinition,
)

from bi_api_connector.connector import (
    BiApiSourceDefinition,
    BiApiConnectionDefinition,
    BiApiConnector,
)

from bi_api_connector.api_schema.source_base import SQLDataSourceSchema, SQLDataSourceTemplateSchema

from bi_connector_clickhouse.formula.constants import DIALECT_NAME_CLICKHOUSE

from bi_connector_bundle_ch_filtered_ya_cloud.ch_geo_filtered.bi.api_schema.connection import (
    CHGeoFilteredConnectionSchema,
)
from bi_connector_bundle_ch_filtered_ya_cloud.ch_geo_filtered.bi.connection_form.form_config import (
    CHGeoFilteredFormFactory,
)
from bi_connector_bundle_ch_filtered_ya_cloud.ch_geo_filtered.bi.connection_info import (
    CHGeoFilteredConnectionInfoProvider,
)
from bi_connector_bundle_ch_filtered_ya_cloud.base.bi.i18n.localizer import CONFIGS


class ClickhouseGeoFilteredBiApiTableSourceDefinition(BiApiSourceDefinition):
    core_source_def_cls = ClickhouseGeoFilteredTableCoreSourceDefinition
    api_schema_cls = SQLDataSourceSchema
    template_api_schema_cls = SQLDataSourceTemplateSchema


class ClickhouseGeoFilteredBiApiConnectionDefinition(BiApiConnectionDefinition):
    core_conn_def_cls = ClickhouseGeoFilteredCoreConnectionDefinition
    api_generic_schema_cls = CHGeoFilteredConnectionSchema
    info_provider_cls = CHGeoFilteredConnectionInfoProvider
    form_factory_cls = CHGeoFilteredFormFactory


class ClickhouseGeoFilteredBiApiConnector(BiApiConnector):
    core_connector_cls = ClickhouseGeoFilteredCoreConnector
    connection_definitions = (ClickhouseGeoFilteredBiApiConnectionDefinition,)
    source_definitions = (ClickhouseGeoFilteredBiApiTableSourceDefinition,)
    formula_dialect_name = DIALECT_NAME_CLICKHOUSE
    translation_configs = frozenset(CONFIGS)
