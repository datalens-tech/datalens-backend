from __future__ import annotations

from bi_connector_bundle_partners.equeo.core.connector import (
    EqueoCoreConnectionDefinition,
    EqueoTableCoreSourceDefinition,
    EqueoCoreConnector,
)

from bi_formula.core.dialect import DialectName

from bi_api_connector.api_schema.source import SQLDataSourceSchema, SQLDataSourceTemplateSchema
from bi_api_connector.connector import (
    BiApiConnectionDefinition, BiApiConnector, BiApiSourceDefinition,
)

from bi_connector_bundle_partners.base.bi.i18n.localizer import CONFIGS
from bi_connector_bundle_partners.equeo.bi.api_schema.connection import EqueoConnectionSchema
from bi_connector_bundle_partners.equeo.bi.connection_form.form_config import EqueoConnectionFormFactory
from bi_connector_bundle_partners.equeo.bi.connection_info import EqueoConnectionInfoProvider


class EqueoTableBiApiSourceDefinition(BiApiSourceDefinition):
    core_source_def_cls = EqueoTableCoreSourceDefinition
    api_schema_cls = SQLDataSourceSchema
    template_api_schema_cls = SQLDataSourceTemplateSchema


class EqueoBiApiConnectionDefinition(BiApiConnectionDefinition):
    core_conn_def_cls = EqueoCoreConnectionDefinition
    api_generic_schema_cls = EqueoConnectionSchema
    form_factory_cls = EqueoConnectionFormFactory
    info_provider_cls = EqueoConnectionInfoProvider


class EqueoBiApiConnector(BiApiConnector):
    core_connector_cls = EqueoCoreConnector
    formula_dialect_name = DialectName.CLICKHOUSE
    connection_definitions = (
        EqueoBiApiConnectionDefinition,
    )
    source_definitions = (
        EqueoTableBiApiSourceDefinition,
    )
    translation_configs = frozenset(CONFIGS)
