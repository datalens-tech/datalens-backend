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

from bi_connector_bundle_ch_filtered.base.bi.i18n.localizer import CONFIGS
from bi_connector_bundle_ch_filtered.usage_tracking.bi.api_schema.connection import UsageTrackingConnectionSchema
from bi_connector_bundle_ch_filtered.usage_tracking.bi.connection_info import UsageTrackingConnectionInfoProvider
from bi_connector_bundle_ch_filtered.usage_tracking.core.connector import (
    UsageTrackingCoreConnectionDefinition,
    UsageTrackingCoreConnector,
    UsageTrackingCoreSourceDefinition,
)


class UsageTrackingApiConnectionDefinition(ApiConnectionDefinition):
    core_conn_def_cls = UsageTrackingCoreConnectionDefinition
    api_generic_schema_cls = UsageTrackingConnectionSchema
    info_provider_cls = UsageTrackingConnectionInfoProvider


class UsageTrackingApiSourceDefinition(ApiSourceDefinition):
    core_source_def_cls = UsageTrackingCoreSourceDefinition
    api_schema_cls = SQLDataSourceSchema
    template_api_schema_cls = SQLDataSourceTemplateSchema


class UsageTrackingApiConnector(ApiConnector):
    core_connector_cls = UsageTrackingCoreConnector
    formula_dialect_name = DIALECT_NAME_CLICKHOUSE
    connection_definitions = (UsageTrackingApiConnectionDefinition,)
    source_definitions = (UsageTrackingApiSourceDefinition,)
    translation_configs = frozenset(CONFIGS)
