from typing import (
    Collection,
    Optional,
    Type,
)

from dl_constants.enums import SourceBackendType
from dl_core.fields import ResultSchema
from dl_formula.core.dialect import DialectCombo
from dl_query_processing.compilation.filter_compiler import (
    FilterFormulaCompiler,
    MainFilterFormulaCompiler,
)
from dl_query_processing.legacy_pipeline.planning import planner
from dl_query_processing.legacy_pipeline.planning.planner import ExecutionPlanner
from dl_query_processing.multi_query.factory import (
    DefaultMultiQueryMutatorFactory,
    MultiQueryMutatorFactoryBase,
)


_FILTER_FORMULA_COMPILER_BY_BACKEND: dict[SourceBackendType, Type[FilterFormulaCompiler]] = {}
_DEFAULT_FILTER_FORMULA_COMPILER = MainFilterFormulaCompiler


def get_filter_formula_compiler_cls(backend_type: SourceBackendType) -> Type[FilterFormulaCompiler]:
    return _FILTER_FORMULA_COMPILER_BY_BACKEND.get(backend_type, _DEFAULT_FILTER_FORMULA_COMPILER)


def register_filter_formula_compiler_cls(
    backend_type: SourceBackendType,
    filter_compiler_cls: Type[FilterFormulaCompiler],
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


_IS_COMPENG_EXECUTABLE_BACKEND_TYPE: dict[SourceBackendType, bool] = {}
_IS_COMPENG_EXECUTABLE_DEFAULT = False


def is_compeng_executable(backend_type: SourceBackendType) -> bool:
    return _IS_COMPENG_EXECUTABLE_BACKEND_TYPE.get(backend_type, _IS_COMPENG_EXECUTABLE_DEFAULT)


def register_is_compeng_executable(backend_type: SourceBackendType, is_compeng_executable: bool) -> None:
    try:
        assert _IS_COMPENG_EXECUTABLE_BACKEND_TYPE[backend_type] is is_compeng_executable
    except KeyError:
        _IS_COMPENG_EXECUTABLE_BACKEND_TYPE[backend_type] = is_compeng_executable


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


_COMPENG_DIALECT: set[DialectCombo] = set()


def get_compeng_dialect() -> DialectCombo:
    assert len(_COMPENG_DIALECT) == 1, "No compeng dialect has been found"
    return next(iter(_COMPENG_DIALECT))


def is_compeng_enabled() -> bool:
    return bool(_COMPENG_DIALECT)


def register_compeng_dialect(dialect: DialectCombo) -> None:
    if not _COMPENG_DIALECT:
        _COMPENG_DIALECT.add(dialect)
    else:
        assert next(iter(_COMPENG_DIALECT)) == dialect
