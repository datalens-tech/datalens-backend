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

from bi_connector_bundle_ch_filtered.base.api.i18n.localizer import CONFIGS
from bi_connector_bundle_ch_filtered.ch_billing_analytics.api.api_schema.connection import (
    CHBillingAnalyticsConnectionSchema,
)
from bi_connector_bundle_ch_filtered.ch_billing_analytics.api.connection_form.form_config import (
    CHBillingAnalyticsConnectionFormFactory,
)
from bi_connector_bundle_ch_filtered.ch_billing_analytics.api.connection_info import (
    CHBillingAnalyticsConnectionInfoProvider,
)
from bi_connector_bundle_ch_filtered.ch_billing_analytics.core.connector import (
    CHBillingAnalyticsCoreConnectionDefinition,
    CHBillingAnalyticsCoreConnector,
    CHBillingAnalyticsTableCoreSourceDefinition,
)


class CHBillingAnalyticsApiConnectionDefinition(ApiConnectionDefinition):
    core_conn_def_cls = CHBillingAnalyticsCoreConnectionDefinition
    api_generic_schema_cls = CHBillingAnalyticsConnectionSchema
    info_provider_cls = CHBillingAnalyticsConnectionInfoProvider
    form_factory_cls = CHBillingAnalyticsConnectionFormFactory


class CHBillingAnalyticsApiSourceDefinition(ApiSourceDefinition):
    core_source_def_cls = CHBillingAnalyticsTableCoreSourceDefinition
    api_schema_cls = SQLDataSourceSchema
    template_api_schema_cls = SQLDataSourceTemplateSchema


class CHBillingAnalyticsApiConnector(ApiConnector):
    core_connector_cls = CHBillingAnalyticsCoreConnector
    formula_dialect_name = DIALECT_NAME_CLICKHOUSE
    connection_definitions = (CHBillingAnalyticsApiConnectionDefinition,)
    source_definitions = (CHBillingAnalyticsApiSourceDefinition,)
    translation_configs = frozenset(CONFIGS)
