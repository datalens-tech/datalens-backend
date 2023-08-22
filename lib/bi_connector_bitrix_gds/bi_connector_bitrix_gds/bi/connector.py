from __future__ import annotations

from bi_connector_bitrix_gds.core.connector import (
    BitrixGDSCoreSourceDefinition, BitrixGDSCoreConnectionDefinition, BitrixGDSCoreConnector,
)

from bi_formula.core.dialect import DialectName

from bi_api_connector.connector import (
    BiApiSourceDefinition, BiApiConnectionDefinition, BiApiConnector,
)
from bi_api_connector.api_schema.source_base import (
    SQLDataSourceSchema, SQLDataSourceTemplateSchema,
)

from bi_connector_bitrix_gds.bi.api_schema.connection import BitrixGDSConnectionSchema
from bi_connector_bitrix_gds.bi.connection_form.form_config import BitrixGDSConnectionFormFactory
from bi_connector_bitrix_gds.bi.connection_info import BitrixGDSConnectionInfoProvider
from bi_connector_bitrix_gds.bi.filter_compiler import BitrixGDSFilterFormulaCompiler
from bi_connector_bitrix_gds.bi.i18n.localizer import CONFIGS
from bi_connector_bitrix_gds.bi.multi_query import BitrixGDSMultiQueryMutatorFactory
from bi_connector_bitrix_gds.bi.planner import BitrixGDSCompengExecutionPlanner


class BitrixGDSBiApiSourceDefinition(BiApiSourceDefinition):
    core_source_def_cls = BitrixGDSCoreSourceDefinition
    api_schema_cls = SQLDataSourceSchema
    template_api_schema_cls = SQLDataSourceTemplateSchema


class BitrixGDSBiApiConnectionDefinition(BiApiConnectionDefinition):
    core_conn_def_cls = BitrixGDSCoreConnectionDefinition
    api_generic_schema_cls = BitrixGDSConnectionSchema
    form_factory_cls = BitrixGDSConnectionFormFactory
    info_provider_cls = BitrixGDSConnectionInfoProvider


class BitrixGDSBiApiConnector(BiApiConnector):
    core_connector_cls = BitrixGDSCoreConnector
    formula_dialect_name = DialectName.BITRIX
    default_multi_query_mutator_factory_cls = BitrixGDSMultiQueryMutatorFactory
    connection_definitions = (
        BitrixGDSBiApiConnectionDefinition,
    )
    source_definitions = (
        BitrixGDSBiApiSourceDefinition,
    )
    legacy_initial_planner_cls = BitrixGDSCompengExecutionPlanner
    is_forkable = False
    filter_formula_compiler_cls = BitrixGDSFilterFormulaCompiler
    translation_configs = frozenset(CONFIGS)
