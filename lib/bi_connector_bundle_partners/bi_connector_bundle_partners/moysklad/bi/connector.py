from __future__ import annotations

from bi_connector_bundle_partners.moysklad.core.connector import (
    MoySkladCoreConnectionDefinition,
    MoySkladTableCoreSourceDefinition,
    MoySkladCoreConnector,
)

from bi_api_connector.api_schema.source import SQLDataSourceSchema, SQLDataSourceTemplateSchema
from bi_api_connector.connector import (
    BiApiConnectionDefinition, BiApiConnector, BiApiSourceDefinition,
)

from bi_connector_clickhouse.formula.constants import DIALECT_NAME_CLICKHOUSE

from bi_connector_bundle_partners.base.bi.i18n.localizer import CONFIGS
from bi_connector_bundle_partners.moysklad.bi.api_schema.connection import MoySkladConnectionSchema
from bi_connector_bundle_partners.moysklad.bi.connection_form.form_config import MoySkladConnectionFormFactory
from bi_connector_bundle_partners.moysklad.bi.connection_info import MoySkladConnectionInfoProvider


class MoySkladTableBiApiSourceDefinition(BiApiSourceDefinition):
    core_source_def_cls = MoySkladTableCoreSourceDefinition
    api_schema_cls = SQLDataSourceSchema
    template_api_schema_cls = SQLDataSourceTemplateSchema


class MoySkladBiApiConnectionDefinition(BiApiConnectionDefinition):
    core_conn_def_cls = MoySkladCoreConnectionDefinition
    api_generic_schema_cls = MoySkladConnectionSchema
    form_factory_cls = MoySkladConnectionFormFactory
    info_provider_cls = MoySkladConnectionInfoProvider


class MoySkladBiApiConnector(BiApiConnector):
    core_connector_cls = MoySkladCoreConnector
    formula_dialect_name = DIALECT_NAME_CLICKHOUSE
    connection_definitions = (
        MoySkladBiApiConnectionDefinition,
    )
    source_definitions = (
        MoySkladTableBiApiSourceDefinition,
    )
    translation_configs = frozenset(CONFIGS)
