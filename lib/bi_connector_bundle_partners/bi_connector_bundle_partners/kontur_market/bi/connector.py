from __future__ import annotations

from bi_connector_bundle_partners.kontur_market.core.connector import (
    KonturMarketCoreConnectionDefinition,
    KonturMarketTableCoreSourceDefinition,
    KonturMarketCoreConnector,
)

from bi_api_connector.api_schema.source import SQLDataSourceSchema, SQLDataSourceTemplateSchema
from bi_api_connector.connector import (
    BiApiConnectionDefinition, BiApiConnector, BiApiSourceDefinition,
)

from bi_connector_clickhouse.formula.constants import DIALECT_NAME_CLICKHOUSE

from bi_connector_bundle_partners.base.bi.i18n.localizer import CONFIGS
from bi_connector_bundle_partners.kontur_market.bi.api_schema.connection import KonturMarketConnectionSchema
from bi_connector_bundle_partners.kontur_market.bi.connection_form.form_config import KonturMarketConnectionFormFactory
from bi_connector_bundle_partners.kontur_market.bi.connection_info import KonturMarketConnectionInfoProvider


class KonturMarketTableBiApiSourceDefinition(BiApiSourceDefinition):
    core_source_def_cls = KonturMarketTableCoreSourceDefinition
    api_schema_cls = SQLDataSourceSchema
    template_api_schema_cls = SQLDataSourceTemplateSchema


class KonturMarketBiApiConnectionDefinition(BiApiConnectionDefinition):
    core_conn_def_cls = KonturMarketCoreConnectionDefinition
    api_generic_schema_cls = KonturMarketConnectionSchema
    form_factory_cls = KonturMarketConnectionFormFactory
    info_provider_cls = KonturMarketConnectionInfoProvider


class KonturMarketBiApiConnector(BiApiConnector):
    core_connector_cls = KonturMarketCoreConnector
    formula_dialect_name = DIALECT_NAME_CLICKHOUSE
    connection_definitions = (
        KonturMarketBiApiConnectionDefinition,
    )
    source_definitions = (
        KonturMarketTableBiApiSourceDefinition,
    )
    translation_configs = frozenset(CONFIGS)
