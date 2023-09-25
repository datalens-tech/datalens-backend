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
from bi_connector_bundle_partners.equeo.bi.api_schema.connection import EqueoConnectionSchema
from bi_connector_bundle_partners.equeo.bi.connection_form.form_config import EqueoConnectionFormFactory
from bi_connector_bundle_partners.equeo.bi.connection_info import EqueoConnectionInfoProvider
from bi_connector_bundle_partners.equeo.core.connector import (
    EqueoCoreConnectionDefinition,
    EqueoCoreConnector,
    EqueoTableCoreSourceDefinition,
)


class EqueoTableApiSourceDefinition(ApiSourceDefinition):
    core_source_def_cls = EqueoTableCoreSourceDefinition
    api_schema_cls = SQLDataSourceSchema
    template_api_schema_cls = SQLDataSourceTemplateSchema


class EqueoApiConnectionDefinition(ApiConnectionDefinition):
    core_conn_def_cls = EqueoCoreConnectionDefinition
    api_generic_schema_cls = EqueoConnectionSchema
    form_factory_cls = EqueoConnectionFormFactory
    info_provider_cls = EqueoConnectionInfoProvider


class EqueoApiConnector(ApiConnector):
    core_connector_cls = EqueoCoreConnector
    formula_dialect_name = DIALECT_NAME_CLICKHOUSE
    connection_definitions = (EqueoApiConnectionDefinition,)
    source_definitions = (EqueoTableApiSourceDefinition,)
    translation_configs = frozenset(CONFIGS)
