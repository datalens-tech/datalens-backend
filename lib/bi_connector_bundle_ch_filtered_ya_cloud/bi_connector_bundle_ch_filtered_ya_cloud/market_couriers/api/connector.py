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

from bi_connector_bundle_ch_filtered.base.api.i18n.localizer import CONFIGS as BASE_CONFIGS
from bi_connector_bundle_ch_filtered_ya_cloud.base.api.i18n.localizer import CONFIGS
from bi_connector_bundle_ch_filtered_ya_cloud.market_couriers.api.api_schema.connection import (
    CHMarketCouriersConnectionSchema,
)
from bi_connector_bundle_ch_filtered_ya_cloud.market_couriers.api.connection_form.form_config import (
    CHMarketCouriersConnectionFormFactory,
)
from bi_connector_bundle_ch_filtered_ya_cloud.market_couriers.api.connection_info import (
    CHMarketCouriersConnectionInfoProvider,
)
from bi_connector_bundle_ch_filtered_ya_cloud.market_couriers.core.connector import (
    CHMarketCouriersCoreConnectionDefinition,
    CHMarketCouriersCoreConnector,
    CHMarketCouriersCoreSourceDefinition,
)


class CHMarketCouriersApiConnectionDefinition(ApiConnectionDefinition):
    core_conn_def_cls = CHMarketCouriersCoreConnectionDefinition
    api_generic_schema_cls = CHMarketCouriersConnectionSchema
    info_provider_cls = CHMarketCouriersConnectionInfoProvider
    form_factory_cls = CHMarketCouriersConnectionFormFactory


class CHMarketCouriersApiSourceDefinition(ApiSourceDefinition):
    core_source_def_cls = CHMarketCouriersCoreSourceDefinition
    api_schema_cls = SQLDataSourceSchema
    template_api_schema_cls = SQLDataSourceTemplateSchema


class CHMarketCouriersApiConnector(ApiConnector):
    core_connector_cls = CHMarketCouriersCoreConnector
    formula_dialect_name = DIALECT_NAME_CLICKHOUSE
    connection_definitions = (CHMarketCouriersApiConnectionDefinition,)
    source_definitions = (CHMarketCouriersApiSourceDefinition,)
    translation_configs = frozenset(BASE_CONFIGS) | frozenset(CONFIGS)
