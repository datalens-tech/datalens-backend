from dl_api_connector.api_schema.source_base import (
    SQLDataSourceSchema,
    SQLDataSourceTemplateSchema,
)
from dl_api_connector.connector import (
    ApiConnectionDefinition,
    ApiConnector,
    ApiSourceDefinition,
)
from dl_connector_clickhouse.formula.constants import DIALECT_NAME_CLICKHOUSE

from bi_connector_bundle_ch_filtered_ya_cloud.base.api.i18n.localizer import CONFIGS
from bi_connector_bundle_ch_filtered_ya_cloud.ch_geo_filtered.api.api_schema.connection import (
    CHGeoFilteredConnectionSchema,
)
from bi_connector_bundle_ch_filtered_ya_cloud.ch_geo_filtered.api.connection_form.form_config import (
    CHGeoFilteredFormFactory,
)
from bi_connector_bundle_ch_filtered_ya_cloud.ch_geo_filtered.api.connection_info import (
    CHGeoFilteredConnectionInfoProvider,
)
from bi_connector_bundle_ch_filtered_ya_cloud.ch_geo_filtered.core.connector import (
    ClickhouseGeoFilteredCoreConnectionDefinition,
    ClickhouseGeoFilteredCoreConnector,
    ClickhouseGeoFilteredTableCoreSourceDefinition,
)


class ClickhouseGeoFilteredApiTableSourceDefinition(ApiSourceDefinition):
    core_source_def_cls = ClickhouseGeoFilteredTableCoreSourceDefinition
    api_schema_cls = SQLDataSourceSchema
    template_api_schema_cls = SQLDataSourceTemplateSchema


class ClickhouseGeoFilteredApiConnectionDefinition(ApiConnectionDefinition):
    core_conn_def_cls = ClickhouseGeoFilteredCoreConnectionDefinition
    api_generic_schema_cls = CHGeoFilteredConnectionSchema
    info_provider_cls = CHGeoFilteredConnectionInfoProvider
    form_factory_cls = CHGeoFilteredFormFactory


class ClickhouseGeoFilteredApiConnector(ApiConnector):
    core_connector_cls = ClickhouseGeoFilteredCoreConnector
    connection_definitions = (ClickhouseGeoFilteredApiConnectionDefinition,)
    source_definitions = (ClickhouseGeoFilteredApiTableSourceDefinition,)
    formula_dialect_name = DIALECT_NAME_CLICKHOUSE
    translation_configs = frozenset(CONFIGS)
