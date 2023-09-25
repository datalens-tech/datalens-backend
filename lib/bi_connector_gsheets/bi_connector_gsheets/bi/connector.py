from __future__ import annotations

from dl_api_connector.api_schema.source_base import (
    SimpleDataSourceSchema,
    SimpleDataSourceTemplateSchema,
)
from dl_api_connector.connector import (
    ApiConnectionDefinition,
    ApiConnector,
    ApiSourceDefinition,
)
from dl_query_processing.multi_query.factory import SimpleFieldSplitterMultiQueryMutatorFactory

from bi_connector_gsheets.bi.api_schema.connection import GSheetsConnectionSchema
from bi_connector_gsheets.bi.connection_form.form_config import GSheetsConnectionFormFactory
from bi_connector_gsheets.bi.connection_info import GSheetsConnectionInfoProvider
from bi_connector_gsheets.bi.filter_compiler import GSheetsFilterFormulaCompiler
from bi_connector_gsheets.bi.i18n.localizer import CONFIGS
from bi_connector_gsheets.core.connector import (
    GSheetsCoreConnectionDefinition,
    GSheetsCoreConnector,
    GSheetsCoreSourceDefinition,
)
from bi_connector_gsheets.formula.constants import DIALECT_NAME_GSHEETS


class GSheetsApiSourceDefinition(ApiSourceDefinition):
    core_source_def_cls = GSheetsCoreSourceDefinition
    api_schema_cls = SimpleDataSourceSchema
    template_api_schema_cls = SimpleDataSourceTemplateSchema


class GSheetsApiConnectionDefinition(ApiConnectionDefinition):
    core_conn_def_cls = GSheetsCoreConnectionDefinition
    api_generic_schema_cls = GSheetsConnectionSchema
    form_factory_cls = GSheetsConnectionFormFactory
    info_provider_cls = GSheetsConnectionInfoProvider


class GSheetsApiConnector(ApiConnector):
    core_connector_cls = GSheetsCoreConnector
    formula_dialect_name = DIALECT_NAME_GSHEETS
    connection_definitions = (GSheetsApiConnectionDefinition,)
    source_definitions = (GSheetsApiSourceDefinition,)
    default_multi_query_mutator_factory_cls = SimpleFieldSplitterMultiQueryMutatorFactory
    is_forkable = False
    is_compeng_executable = True
    filter_formula_compiler_cls = GSheetsFilterFormulaCompiler
    translation_configs = frozenset(CONFIGS)
