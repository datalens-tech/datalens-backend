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

from bi_connector_bundle_ch_frozen.ch_frozen_base.api.api_schema.connection import BaseClickHouseFrozenConnectionSchema
from bi_connector_bundle_ch_frozen.ch_frozen_base.api.i18n.localizer import CONFIGS
from bi_connector_bundle_ch_frozen.ch_frozen_base.core.connector import (
    CHFrozenBaseCoreConnectionDefinition,
    CHFrozenBaseCoreSourceDefinition,
    CHFrozenCoreConnector,
)


class BaseCHFrozenTableApiSourceDefinition(ApiSourceDefinition):
    core_source_def_cls = CHFrozenBaseCoreSourceDefinition
    api_schema_cls = SQLDataSourceSchema
    template_api_schema_cls = SQLDataSourceTemplateSchema


class BaseCHFrozenApiConnectionDefinition(ApiConnectionDefinition):
    core_conn_def_cls = CHFrozenBaseCoreConnectionDefinition
    api_generic_schema_cls = BaseClickHouseFrozenConnectionSchema


class BaseCHFrozenApiConnector(ApiConnector):
    core_connector_cls = CHFrozenCoreConnector
    formula_dialect_name = DIALECT_NAME_CLICKHOUSE
    connection_definitions = (BaseCHFrozenApiConnectionDefinition,)
    source_definitions = (BaseCHFrozenTableApiSourceDefinition,)
    translation_configs = frozenset(CONFIGS)
