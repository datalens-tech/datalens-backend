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

from bi_connector_bundle_partners.base.bi.i18n.localizer import CONFIGS
from bi_connector_bundle_partners.kontur_market.bi.api_schema.connection import KonturMarketConnectionSchema
from bi_connector_bundle_partners.kontur_market.bi.connection_form.form_config import KonturMarketConnectionFormFactory
from bi_connector_bundle_partners.kontur_market.bi.connection_info import KonturMarketConnectionInfoProvider
from bi_connector_bundle_partners.kontur_market.core.connector import (
    KonturMarketCoreConnectionDefinition,
    KonturMarketCoreConnector,
    KonturMarketTableCoreSourceDefinition,
)


class KonturMarketTableApiSourceDefinition(ApiSourceDefinition):
    core_source_def_cls = KonturMarketTableCoreSourceDefinition
    api_schema_cls = SQLDataSourceSchema
    template_api_schema_cls = SQLDataSourceTemplateSchema


class KonturMarketApiConnectionDefinition(ApiConnectionDefinition):
    core_conn_def_cls = KonturMarketCoreConnectionDefinition
    api_generic_schema_cls = KonturMarketConnectionSchema
    form_factory_cls = KonturMarketConnectionFormFactory
    info_provider_cls = KonturMarketConnectionInfoProvider


class KonturMarketApiConnector(ApiConnector):
    core_connector_cls = KonturMarketCoreConnector
    formula_dialect_name = DIALECT_NAME_CLICKHOUSE
    connection_definitions = (KonturMarketApiConnectionDefinition,)
    source_definitions = (KonturMarketTableApiSourceDefinition,)
    translation_configs = frozenset(CONFIGS)
