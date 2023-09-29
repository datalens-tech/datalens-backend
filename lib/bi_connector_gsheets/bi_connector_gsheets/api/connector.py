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
from dl_api_lib.query.registry import MQMFactorySettingItem
from dl_constants.enums import QueryProcessingMode
from dl_query_processing.multi_query.factory import SimpleFieldSplitterMultiQueryMutatorFactory

from bi_connector_gsheets.api.api_schema.connection import GSheetsConnectionSchema
from bi_connector_gsheets.api.connection_form.form_config import GSheetsConnectionFormFactory
from bi_connector_gsheets.api.connection_info import GSheetsConnectionInfoProvider
from bi_connector_gsheets.api.filter_compiler import GSheetsFilterFormulaCompiler
from bi_connector_gsheets.api.i18n.localizer import CONFIGS
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
    multi_query_mutation_factories = (
        MQMFactorySettingItem(
            query_proc_mode=QueryProcessingMode.basic,
            factory_cls=SimpleFieldSplitterMultiQueryMutatorFactory,
        ),
    )
    is_forkable = False
    is_compeng_executable = True
    filter_formula_compiler_cls = GSheetsFilterFormulaCompiler
    translation_configs = frozenset(CONFIGS)
