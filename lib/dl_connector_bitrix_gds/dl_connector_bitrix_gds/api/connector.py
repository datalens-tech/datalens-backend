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
from dl_api_lib.query.registry import MQMFactorySettingItem
from dl_constants.enums import QueryProcessingMode

from dl_connector_bitrix_gds.api.api_schema.connection import BitrixGDSConnectionSchema
from dl_connector_bitrix_gds.api.connection_form.form_config import BitrixGDSConnectionFormFactory
from dl_connector_bitrix_gds.api.connection_info import BitrixGDSConnectionInfoProvider
from dl_connector_bitrix_gds.api.filter_compiler import BitrixGDSFilterFormulaCompiler
from dl_connector_bitrix_gds.api.i18n.localizer import CONFIGS
from dl_connector_bitrix_gds.api.multi_query import BitrixGDSMultiQueryMutatorFactory
from dl_connector_bitrix_gds.core.connector import (
    BitrixGDSCoreConnectionDefinition,
    BitrixGDSCoreConnector,
    BitrixGDSCoreSourceDefinition,
)
from dl_connector_bitrix_gds.formula.constants import DIALECT_NAME_BITRIX


class BitrixGDSApiSourceDefinition(ApiSourceDefinition):
    core_source_def_cls = BitrixGDSCoreSourceDefinition
    api_schema_cls = SQLDataSourceSchema
    template_api_schema_cls = SQLDataSourceTemplateSchema


class BitrixGDSApiConnectionDefinition(ApiConnectionDefinition):
    core_conn_def_cls = BitrixGDSCoreConnectionDefinition
    api_generic_schema_cls = BitrixGDSConnectionSchema
    form_factory_cls = BitrixGDSConnectionFormFactory
    info_provider_cls = BitrixGDSConnectionInfoProvider


class BitrixGDSApiConnector(ApiConnector):
    core_connector_cls = BitrixGDSCoreConnector
    formula_dialect_name = DIALECT_NAME_BITRIX
    multi_query_mutation_factories = (
        MQMFactorySettingItem(
            query_proc_mode=QueryProcessingMode.basic,
            factory_cls=BitrixGDSMultiQueryMutatorFactory,
        ),
    )
    connection_definitions = (BitrixGDSApiConnectionDefinition,)
    source_definitions = (BitrixGDSApiSourceDefinition,)
    is_forkable = False
    is_compeng_executable = True
    filter_formula_compiler_cls = BitrixGDSFilterFormulaCompiler
    translation_configs = frozenset(CONFIGS)
