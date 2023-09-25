from __future__ import annotations

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

from bi_connector_bundle_ch_filtered.base.bi.i18n.localizer import CONFIGS as BASE_CONFIGS
from bi_connector_bundle_ch_filtered_ya_cloud.base.bi.i18n.localizer import CONFIGS
from bi_connector_bundle_ch_filtered_ya_cloud.smb_heatmaps.bi.api_schema.connection import CHSMBHeatmapsConnectionSchema
from bi_connector_bundle_ch_filtered_ya_cloud.smb_heatmaps.bi.connection_form.form_config import (
    CHSMBHeatmapsConnectionFormFactory,
)
from bi_connector_bundle_ch_filtered_ya_cloud.smb_heatmaps.bi.connection_info import CHSMBHeatmapsConnectionInfoProvider
from bi_connector_bundle_ch_filtered_ya_cloud.smb_heatmaps.core.connector import (
    CHSMBHeatmapsCoreConnectionDefinition,
    CHSMBHeatmapsCoreConnector,
    CHSMBHeatmapsCoreSourceDefinition,
)


class CHSMBHeatmapsApiConnectionDefinition(ApiConnectionDefinition):
    core_conn_def_cls = CHSMBHeatmapsCoreConnectionDefinition
    api_generic_schema_cls = CHSMBHeatmapsConnectionSchema
    info_provider_cls = CHSMBHeatmapsConnectionInfoProvider
    form_factory_cls = CHSMBHeatmapsConnectionFormFactory


class CHSMBHeatmapsApiSourceDefinition(ApiSourceDefinition):
    core_source_def_cls = CHSMBHeatmapsCoreSourceDefinition
    api_schema_cls = SQLDataSourceSchema
    template_api_schema_cls = SQLDataSourceTemplateSchema


class CHSMBHeatmapsApiConnector(ApiConnector):
    core_connector_cls = CHSMBHeatmapsCoreConnector
    formula_dialect_name = DIALECT_NAME_CLICKHOUSE
    connection_definitions = (CHSMBHeatmapsApiConnectionDefinition,)
    source_definitions = (CHSMBHeatmapsApiSourceDefinition,)
    translation_configs = frozenset(BASE_CONFIGS) | frozenset(CONFIGS)
