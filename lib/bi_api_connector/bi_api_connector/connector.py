from typing import ClassVar, Tuple, Type, Optional

import abc

from bi_core.connectors.base.connector import (
    CoreConnector, CoreSourceDefinition, CoreConnectionDefinition
)
from bi_core.i18n.localizer_base import TranslationConfig

from bi_formula.core.dialect import DialectName

from bi_query_processing.compilation.filter_compiler import FilterFormulaCompiler, MainFilterFormulaCompiler
from bi_query_processing.legacy_pipeline.planning.planner import ExecutionPlanner, WindowToCompengExecutionPlanner
from bi_query_processing.multi_query.factory import MultiQueryMutatorFactoryBase, DefaultMultiQueryMutatorFactory

from bi_api_connector.api_schema.source import DataSourceBaseSchema, DataSourceTemplateBaseSchema
from bi_api_connector.api_schema.connection_base import ConnectionSchema
from bi_api_connector.connection_info import ConnectionInfoProvider
from bi_api_connector.form_config.models.base import ConnectionFormFactory


class BiApiSourceDefinition(abc.ABC):
    core_source_def_cls: ClassVar[Type[CoreSourceDefinition]]
    api_schema_cls: ClassVar[Type[DataSourceBaseSchema]] = DataSourceBaseSchema
    template_api_schema_cls: ClassVar[Type[DataSourceTemplateBaseSchema]] = DataSourceTemplateBaseSchema


class BiApiConnectionDefinition(abc.ABC):
    core_conn_def_cls: ClassVar[Type[CoreConnectionDefinition]]
    api_generic_schema_cls: ClassVar[Type[ConnectionSchema]]
    alias: ClassVar[Optional[str]] = None
    info_provider_cls: ClassVar[Type[ConnectionInfoProvider]]
    form_factory_cls: ClassVar[Optional[Type[ConnectionFormFactory]]] = None


class BiApiConnector(abc.ABC):
    core_connector_cls: ClassVar[Type[CoreConnector]]
    connection_definitions: ClassVar[Tuple[Type[BiApiConnectionDefinition], ...]] = ()
    source_definitions: ClassVar[Tuple[Type[BiApiSourceDefinition], ...]] = ()
    formula_dialect_name: ClassVar[DialectName] = DialectName.DUMMY
    default_multi_query_mutator_factory_cls: ClassVar[Type[MultiQueryMutatorFactoryBase]] = (
        DefaultMultiQueryMutatorFactory
    )
    legacy_initial_planner_cls: ClassVar[Type[ExecutionPlanner]] = WindowToCompengExecutionPlanner  # TODO: Remove with old LODs
    is_forkable: ClassVar[bool] = True
    filter_formula_compiler_cls: ClassVar[Type[FilterFormulaCompiler]] = MainFilterFormulaCompiler
    translation_configs: ClassVar[frozenset[TranslationConfig]] = frozenset()
