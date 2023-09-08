import os
from typing import Collection, Optional, Type

from bi_constants.enums import SourceBackendType

from bi_core.fields import ResultSchema

from bi_formula.core.dialect import DialectCombo

from bi_query_processing.compilation.filter_compiler import FilterFormulaCompiler, MainFilterFormulaCompiler

from bi_query_processing.legacy_pipeline.planning import planner
from bi_query_processing.legacy_pipeline.planning.planner import ExecutionPlanner
from bi_query_processing.multi_query.factory import (
    MultiQueryMutatorFactoryBase, DefaultMultiQueryMutatorFactory,
    DefaultNativeWFMultiQueryMutatorFactory,
)

# FIXME: Remove connectors
from bi_connector_mysql.core.constants import BACKEND_TYPE_MYSQL
from bi_connector_mysql.formula.constants import MySQLDialect
from bi_connector_clickhouse.core.constants import BACKEND_TYPE_CLICKHOUSE
from bi_connector_clickhouse.formula.constants import ClickHouseDialect
from bi_connector_postgresql.core.postgresql.constants import BACKEND_TYPE_POSTGRES
from bi_connector_postgresql.formula.constants import PostgreSQLDialect


_FILTER_FORMULA_COMPILER_BY_BACKEND: dict[SourceBackendType, Type[FilterFormulaCompiler]] = {}
_DEFAULT_FILTER_FORMULA_COMPILER = MainFilterFormulaCompiler


def get_filter_formula_compiler_cls(backend_type: SourceBackendType) -> Type[FilterFormulaCompiler]:
    return _FILTER_FORMULA_COMPILER_BY_BACKEND.get(backend_type, _DEFAULT_FILTER_FORMULA_COMPILER)


def register_filter_formula_compiler_cls(
        backend_type: SourceBackendType, filter_compiler_cls: Type[FilterFormulaCompiler],
) -> None:
    try:
        assert _FILTER_FORMULA_COMPILER_BY_BACKEND[backend_type] is filter_compiler_cls
    except KeyError:
        _FILTER_FORMULA_COMPILER_BY_BACKEND[backend_type] = filter_compiler_cls


_PLANNERS_BY_BACKEND: dict[SourceBackendType, Type[ExecutionPlanner]] = {}
_DEFAULT_PLANNER: Type[ExecutionPlanner] = planner.WindowToCompengExecutionPlanner


def get_initial_planner_cls(backend_type: SourceBackendType) -> Type[ExecutionPlanner]:
    return _PLANNERS_BY_BACKEND.get(backend_type, _DEFAULT_PLANNER)


def register_initial_planner_cls(backend_type: SourceBackendType, planner_cls: Type[ExecutionPlanner]) -> None:
    try:
        assert _PLANNERS_BY_BACKEND[backend_type] is planner_cls
    except KeyError:
        _PLANNERS_BY_BACKEND[backend_type] = planner_cls


_IS_FORKABLE_BACKEND_TYPE: dict[SourceBackendType, bool] = {}
_IS_FORKABLE_DEFAULT = True


def is_forkable_source(backend_type: SourceBackendType) -> bool:
    return _IS_FORKABLE_BACKEND_TYPE.get(backend_type, _IS_FORKABLE_DEFAULT)


def register_is_forkable_source(backend_type: SourceBackendType, is_forkable: bool) -> None:
    try:
        assert _IS_FORKABLE_BACKEND_TYPE[backend_type] is is_forkable
    except KeyError:
        _IS_FORKABLE_BACKEND_TYPE[backend_type] = is_forkable


_MQM_FACTORY_REGISTRY: dict[tuple[SourceBackendType, Optional[DialectCombo]], Type[MultiQueryMutatorFactoryBase]] = {}


def get_multi_query_mutator_factory(
        backend_type: SourceBackendType,
        dialect: DialectCombo,
        result_schema: ResultSchema,
) -> MultiQueryMutatorFactoryBase:
    factory_cls = _MQM_FACTORY_REGISTRY.get(
        (backend_type, dialect),  # First try with exact dialect
        _MQM_FACTORY_REGISTRY.get(
            (backend_type, None),  # Then try without the dialect, just the backend
            DefaultMultiQueryMutatorFactory,  # If still nothing, then use the default
        ),
    )
    return factory_cls(result_schema=result_schema)


def register_multi_query_mutator_factory_cls(
        backend_type: SourceBackendType,
        dialects: Collection[Optional[DialectCombo]],
        factory_cls: Type[MultiQueryMutatorFactoryBase],
) -> None:
    for dialect in dialects:
        key = (backend_type, dialect)
        if key in _MQM_FACTORY_REGISTRY:
            assert _MQM_FACTORY_REGISTRY[key] is factory_cls
        else:
            _MQM_FACTORY_REGISTRY[key] = factory_cls


def register_for_connectors_with_native_wf() -> None:
    """
    Register factories for connector/dialect combinations
    that support native window functions

    TODO: Connectorize and get rid of os.environ
    """

    if os.environ.get('NATIVE_WF_POSTGRESQL', '0') == '1':
        register_multi_query_mutator_factory_cls(
            backend_type=BACKEND_TYPE_POSTGRES,
            dialects=PostgreSQLDialect.and_above(PostgreSQLDialect.POSTGRESQL_9_4).to_list(),
            factory_cls=DefaultNativeWFMultiQueryMutatorFactory,
        )
    if os.environ.get('NATIVE_WF_CLICKHOUSE', '0') == '1':
        register_multi_query_mutator_factory_cls(
            backend_type=BACKEND_TYPE_CLICKHOUSE,
            dialects=ClickHouseDialect.and_above(ClickHouseDialect.CLICKHOUSE_22_10).to_list(),
            factory_cls=DefaultNativeWFMultiQueryMutatorFactory,
        )
    if os.environ.get('NATIVE_WF_MYSQL', '0') == '1':
        register_multi_query_mutator_factory_cls(
            backend_type=BACKEND_TYPE_MYSQL,
            dialects=MySQLDialect.and_above(MySQLDialect.MYSQL_8_0_12).to_list(),
            factory_cls=DefaultNativeWFMultiQueryMutatorFactory,
        )


# Native window functions
register_for_connectors_with_native_wf()
