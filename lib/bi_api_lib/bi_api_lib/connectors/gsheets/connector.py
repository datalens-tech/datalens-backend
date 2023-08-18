from __future__ import annotations

from bi_connector_gsheets.core.connector import (
    GSheetsCoreSourceDefinition, GSheetsCoreConnectionDefinition, GSheetsCoreConnector,
)

from bi_formula.core.dialect import DialectName

from bi_api_connector.connector import (
    BiApiSourceDefinition, BiApiConnectionDefinition, BiApiConnector,
)
from bi_api_connector.api_schema.source_base import (
    SimpleDataSourceSchema, SimpleDataSourceTemplateSchema,
)

from bi_query_processing.multi_query.factory import SimpleFieldSplitterMultiQueryMutatorFactory

from bi_api_lib.connectors.gsheets.connection_form.form_config import GSheetsConnectionFormFactory
from bi_api_lib.connectors.gsheets.connection_info import GSheetsConnectionInfoProvider
from bi_api_lib.connectors.gsheets.schemas import GSheetsConnectionSchema
from bi_api_lib.connectors.gsheets.filter_compiler import GSheetsFilterFormulaCompiler
from bi_api_lib.connectors.gsheets.planner import GSheetsCompengExecutionPlanner


class GSheetsBiApiSourceDefinition(BiApiSourceDefinition):
    core_source_def_cls = GSheetsCoreSourceDefinition
    api_schema_cls = SimpleDataSourceSchema
    template_api_schema_cls = SimpleDataSourceTemplateSchema


class GSheetsBiApiConnectionDefinition(BiApiConnectionDefinition):
    core_conn_def_cls = GSheetsCoreConnectionDefinition
    api_generic_schema_cls = GSheetsConnectionSchema
    form_factory_cls = GSheetsConnectionFormFactory
    info_provider_cls = GSheetsConnectionInfoProvider


class GSheetsBiApiConnector(BiApiConnector):
    core_connector_cls = GSheetsCoreConnector
    formula_dialect_name = DialectName.GSHEETS
    connection_definitions = (
        GSheetsBiApiConnectionDefinition,
    )
    source_definitions = (
        GSheetsBiApiSourceDefinition,
    )
    default_multi_query_mutator_factory_cls = SimpleFieldSplitterMultiQueryMutatorFactory
    legacy_initial_planner_cls = GSheetsCompengExecutionPlanner
    is_forkable = False
    filter_formula_compiler_cls = GSheetsFilterFormulaCompiler
