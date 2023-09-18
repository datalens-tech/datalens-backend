from __future__ import annotations

from bi_connector_bundle_ch_filtered_ya_cloud.market_couriers.core.connector import (
    CHMarketCouriersCoreConnectionDefinition,
    CHMarketCouriersCoreSourceDefinition,
    CHMarketCouriersCoreConnector,
)

from dl_api_connector.api_schema.source_base import SQLDataSourceSchema, SQLDataSourceTemplateSchema
from dl_api_connector.connector import (
    BiApiConnectionDefinition, BiApiConnector, BiApiSourceDefinition,
)

from dl_connector_clickhouse.formula.constants import DIALECT_NAME_CLICKHOUSE

from bi_connector_bundle_ch_filtered.base.bi.i18n.localizer import CONFIGS as BASE_CONFIGS
from bi_connector_bundle_ch_filtered_ya_cloud.base.bi.i18n.localizer import CONFIGS
from bi_connector_bundle_ch_filtered_ya_cloud.market_couriers.bi.api_schema.connection import (
    CHMarketCouriersConnectionSchema,
)
from bi_connector_bundle_ch_filtered_ya_cloud.market_couriers.bi.connection_form.form_config import (
    CHMarketCouriersConnectionFormFactory,
)
from bi_connector_bundle_ch_filtered_ya_cloud.market_couriers.bi.connection_info import (
    CHMarketCouriersConnectionInfoProvider,
)


class CHMarketCouriersBiApiConnectionDefinition(BiApiConnectionDefinition):
    core_conn_def_cls = CHMarketCouriersCoreConnectionDefinition
    api_generic_schema_cls = CHMarketCouriersConnectionSchema
    info_provider_cls = CHMarketCouriersConnectionInfoProvider
    form_factory_cls = CHMarketCouriersConnectionFormFactory


class CHMarketCouriersBiApiSourceDefinition(BiApiSourceDefinition):
    core_source_def_cls = CHMarketCouriersCoreSourceDefinition
    api_schema_cls = SQLDataSourceSchema
    template_api_schema_cls = SQLDataSourceTemplateSchema


class CHMarketCouriersBiApiConnector(BiApiConnector):
    core_connector_cls = CHMarketCouriersCoreConnector
    formula_dialect_name = DIALECT_NAME_CLICKHOUSE
    connection_definitions = (
        CHMarketCouriersBiApiConnectionDefinition,
    )
    source_definitions = (
        CHMarketCouriersBiApiSourceDefinition,
    )
    translation_configs = frozenset(BASE_CONFIGS) | frozenset(CONFIGS)
