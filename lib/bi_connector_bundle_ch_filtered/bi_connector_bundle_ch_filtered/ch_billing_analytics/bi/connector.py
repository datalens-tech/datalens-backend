from __future__ import annotations

from bi_connector_bundle_ch_filtered.ch_billing_analytics.core.connector import (
    CHBillingAnalyticsCoreConnectionDefinition,
    CHBillingAnalyticsTableCoreSourceDefinition,
    CHBillingAnalyticsCoreConnector,
)

from bi_api_connector.connector import (
    BiApiConnectionDefinition, BiApiConnector, BiApiSourceDefinition,
)
from bi_api_connector.api_schema.source import SQLDataSourceSchema, SQLDataSourceTemplateSchema

from bi_connector_clickhouse.formula.constants import DIALECT_NAME_CLICKHOUSE

from bi_connector_bundle_ch_filtered.base.bi.i18n.localizer import CONFIGS
from bi_connector_bundle_ch_filtered.ch_billing_analytics.bi.api_schema.connection import (
    CHBillingAnalyticsConnectionSchema,
)
from bi_connector_bundle_ch_filtered.ch_billing_analytics.bi.connection_form.form_config import (
    CHBillingAnalyticsConnectionFormFactory,
)
from bi_connector_bundle_ch_filtered.ch_billing_analytics.bi.connection_info import (
    CHBillingAnalyticsConnectionInfoProvider,
)


class CHBillingAnalyticsBiApiConnectionDefinition(BiApiConnectionDefinition):
    core_conn_def_cls = CHBillingAnalyticsCoreConnectionDefinition
    api_generic_schema_cls = CHBillingAnalyticsConnectionSchema
    info_provider_cls = CHBillingAnalyticsConnectionInfoProvider
    form_factory_cls = CHBillingAnalyticsConnectionFormFactory


class CHBillingAnalyticsBiApiSourceDefinition(BiApiSourceDefinition):
    core_source_def_cls = CHBillingAnalyticsTableCoreSourceDefinition
    api_schema_cls = SQLDataSourceSchema
    template_api_schema_cls = SQLDataSourceTemplateSchema


class CHBillingAnalyticsBiApiConnector(BiApiConnector):
    core_connector_cls = CHBillingAnalyticsCoreConnector
    formula_dialect_name = DIALECT_NAME_CLICKHOUSE
    connection_definitions = (
        CHBillingAnalyticsBiApiConnectionDefinition,
    )
    source_definitions = (
        CHBillingAnalyticsBiApiSourceDefinition,
    )
    translation_configs = frozenset(CONFIGS)
