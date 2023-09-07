from __future__ import annotations

from bi_connector_bundle_ch_frozen.ch_frozen_base.core.connector import (
    CHFrozenBaseCoreSourceDefinition,
    CHFrozenBaseCoreConnectionDefinition,
    CHFrozenCoreConnector,
)

from bi_api_connector.connector import (
    BiApiConnectionDefinition, BiApiConnector, BiApiSourceDefinition,
)
from bi_api_connector.api_schema.source_base import SQLDataSourceSchema, SQLDataSourceTemplateSchema

from bi_connector_clickhouse.formula.constants import DIALECT_NAME_CLICKHOUSE

from bi_connector_bundle_ch_frozen.ch_frozen_base.bi.api_schema.connection import BaseClickHouseFrozenConnectionSchema
from bi_connector_bundle_ch_frozen.ch_frozen_base.bi.i18n.localizer import CONFIGS


class BaseCHFrozenTableBiApiSourceDefinition(BiApiSourceDefinition):
    core_source_def_cls = CHFrozenBaseCoreSourceDefinition
    api_schema_cls = SQLDataSourceSchema
    template_api_schema_cls = SQLDataSourceTemplateSchema


class BaseCHFrozenBiApiConnectionDefinition(BiApiConnectionDefinition):
    core_conn_def_cls = CHFrozenBaseCoreConnectionDefinition
    api_generic_schema_cls = BaseClickHouseFrozenConnectionSchema


class BaseCHFrozenBiApiConnector(BiApiConnector):
    core_connector_cls = CHFrozenCoreConnector
    formula_dialect_name = DIALECT_NAME_CLICKHOUSE
    connection_definitions = (
        BaseCHFrozenBiApiConnectionDefinition,
    )
    source_definitions = (
        BaseCHFrozenTableBiApiSourceDefinition,
    )
    translation_configs = frozenset(CONFIGS)
