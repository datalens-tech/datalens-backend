from __future__ import annotations

from bi_connector_bundle_ch_filtered.usage_tracking.core.connector import (
    UsageTrackingCoreConnectionDefinition,
    UsageTrackingCoreSourceDefinition,
    UsageTrackingCoreConnector,
)

from bi_api_connector.connector import (
    BiApiConnectionDefinition, BiApiConnector, BiApiSourceDefinition,
)
from bi_api_connector.api_schema.source import SQLDataSourceSchema, SQLDataSourceTemplateSchema

from bi_connector_clickhouse.formula.constants import DIALECT_NAME_CLICKHOUSE

from bi_connector_bundle_ch_filtered.base.bi.i18n.localizer import CONFIGS
from bi_connector_bundle_ch_filtered.usage_tracking.bi.api_schema.connection import UsageTrackingConnectionSchema
from bi_connector_bundle_ch_filtered.usage_tracking.bi.connection_info import UsageTrackingConnectionInfoProvider


class UsageTrackingBiApiConnectionDefinition(BiApiConnectionDefinition):
    core_conn_def_cls = UsageTrackingCoreConnectionDefinition
    api_generic_schema_cls = UsageTrackingConnectionSchema
    info_provider_cls = UsageTrackingConnectionInfoProvider


class UsageTrackingBiApiSourceDefinition(BiApiSourceDefinition):
    core_source_def_cls = UsageTrackingCoreSourceDefinition
    api_schema_cls = SQLDataSourceSchema
    template_api_schema_cls = SQLDataSourceTemplateSchema


class UsageTrackingBiApiConnector(BiApiConnector):
    core_connector_cls = UsageTrackingCoreConnector
    formula_dialect_name = DIALECT_NAME_CLICKHOUSE
    connection_definitions = (
        UsageTrackingBiApiConnectionDefinition,
    )
    source_definitions = (
        UsageTrackingBiApiSourceDefinition,
    )
    translation_configs = frozenset(CONFIGS)
