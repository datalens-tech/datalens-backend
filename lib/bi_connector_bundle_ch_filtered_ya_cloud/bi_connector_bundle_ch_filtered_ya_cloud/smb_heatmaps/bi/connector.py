from __future__ import annotations


from bi_connector_bundle_ch_filtered_ya_cloud.smb_heatmaps.core.connector import (
    CHSMBHeatmapsCoreConnectionDefinition,
    CHSMBHeatmapsCoreSourceDefinition,
    CHSMBHeatmapsCoreConnector,
)

from bi_api_connector.api_schema.source import SQLDataSourceSchema, SQLDataSourceTemplateSchema
from bi_api_connector.connector import (
    BiApiConnectionDefinition, BiApiConnector, BiApiSourceDefinition,
)

from bi_connector_clickhouse.formula.constants import DIALECT_NAME_CLICKHOUSE

from bi_connector_bundle_ch_filtered.base.bi.i18n.localizer import CONFIGS as BASE_CONFIGS
from bi_connector_bundle_ch_filtered_ya_cloud.base.bi.i18n.localizer import CONFIGS
from bi_connector_bundle_ch_filtered_ya_cloud.smb_heatmaps.bi.api_schema.connection import CHSMBHeatmapsConnectionSchema
from bi_connector_bundle_ch_filtered_ya_cloud.smb_heatmaps.bi.connection_form.form_config import (
    CHSMBHeatmapsConnectionFormFactory,
)
from bi_connector_bundle_ch_filtered_ya_cloud.smb_heatmaps.bi.connection_info import CHSMBHeatmapsConnectionInfoProvider


class CHSMBHeatmapsBiApiConnectionDefinition(BiApiConnectionDefinition):
    core_conn_def_cls = CHSMBHeatmapsCoreConnectionDefinition
    api_generic_schema_cls = CHSMBHeatmapsConnectionSchema
    info_provider_cls = CHSMBHeatmapsConnectionInfoProvider
    form_factory_cls = CHSMBHeatmapsConnectionFormFactory


class CHSMBHeatmapsBiApiSourceDefinition(BiApiSourceDefinition):
    core_source_def_cls = CHSMBHeatmapsCoreSourceDefinition
    api_schema_cls = SQLDataSourceSchema
    template_api_schema_cls = SQLDataSourceTemplateSchema


class CHSMBHeatmapsBiApiConnector(BiApiConnector):
    core_connector_cls = CHSMBHeatmapsCoreConnector
    formula_dialect_name = DIALECT_NAME_CLICKHOUSE
    connection_definitions = (
        CHSMBHeatmapsBiApiConnectionDefinition,
    )
    source_definitions = (
        CHSMBHeatmapsBiApiSourceDefinition,
    )
    translation_configs = frozenset(BASE_CONFIGS) | frozenset(CONFIGS)
