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

from bi_connector_usage_tracking_ya_team.bi.api_schema.connection import UsageTrackingYaTeamConnectionSchema
from bi_connector_usage_tracking_ya_team.bi.connection_form.form_config import UsageTrackingYaTeamConnectionFormFactory
from bi_connector_usage_tracking_ya_team.bi.connection_info import UsageTrackingYaTeamConnectionInfoProvider
from bi_connector_usage_tracking_ya_team.bi.i18n.localizer import CONFIGS
from bi_connector_usage_tracking_ya_team.core.connector import (
    UsageTrackingYaTeamCoreConnectionDefinition,
    UsageTrackingYaTeamCoreConnector,
    UsageTrackingYaTeamCoreSourceDefinition,
)


class UsageTrackingYaTeamApiConnectionDefinition(ApiConnectionDefinition):
    core_conn_def_cls = UsageTrackingYaTeamCoreConnectionDefinition
    api_generic_schema_cls = UsageTrackingYaTeamConnectionSchema
    info_provider_cls = UsageTrackingYaTeamConnectionInfoProvider
    form_factory_cls = UsageTrackingYaTeamConnectionFormFactory


class UsageTrackingYaTeamApiSourceDefinition(ApiSourceDefinition):
    core_source_def_cls = UsageTrackingYaTeamCoreSourceDefinition
    api_schema_cls = SQLDataSourceSchema
    template_api_schema_cls = SQLDataSourceTemplateSchema


class UsageTrackingYaTeamApiConnector(ApiConnector):
    core_connector_cls = UsageTrackingYaTeamCoreConnector
    formula_dialect_name = DIALECT_NAME_CLICKHOUSE
    connection_definitions = (UsageTrackingYaTeamApiConnectionDefinition,)
    source_definitions = (UsageTrackingYaTeamApiSourceDefinition,)
    translation_configs = frozenset(CONFIGS)
