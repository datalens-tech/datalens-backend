from typing import (
    Collection,
    Optional,
    Type,
)

from dl_api_connector.connector import MQMFactoryKey
from dl_constants.enums import (
    QueryProcessingMode,
    SourceBackendType,
)
from dl_formula.core.dialect import (
    DialectCombo,
    DialectName,
)
from dl_query_processing.compilation.filter_compiler import (
    FilterFormulaCompiler,
    MainFilterFormulaCompiler,
)
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


_IS_COMPENG_EXECUTABLE_BACKEND_TYPE: dict[SourceBackendType, bool] = {}
_IS_COMPENG_EXECUTABLE_DEFAULT = False


def is_compeng_executable(backend_type: SourceBackendType) -> bool:
    return _IS_COMPENG_EXECUTABLE_BACKEND_TYPE.get(backend_type, _IS_COMPENG_EXECUTABLE_DEFAULT)


def register_is_compeng_executable(backend_type: SourceBackendType, is_compeng_executable: bool) -> None:
    try:
        assert _IS_COMPENG_EXECUTABLE_BACKEND_TYPE[backend_type] is is_compeng_executable
    except KeyError:
        _IS_COMPENG_EXECUTABLE_BACKEND_TYPE[backend_type] = is_compeng_executable


_MQM_FACTORY_REGISTRY: dict[MQMFactoryKey, Type[MultiQueryMutatorFactoryBase]] = {}


def _get_default_mqm_factory_cls() -> Type[MultiQueryMutatorFactoryBase]:
    return DefaultMultiQueryMutatorFactory


def get_multi_query_mutator_factory_class(
    query_proc_mode: QueryProcessingMode,
    backend_type: SourceBackendType,
    dialect: DialectCombo,
) -> Type[MultiQueryMutatorFactoryBase]:
    prioritized_keys = (
        # First try with exact dialect and mode (exact match)
        MQMFactoryKey(query_proc_mode=query_proc_mode, backend_type=backend_type, dialect=dialect),
        # Now the fallbacks begin...
        # Try without the dialect (all dialects within backend), just the backend and mode
        MQMFactoryKey(query_proc_mode=query_proc_mode, backend_type=backend_type, dialect=None),
        # Fall back to `basic` mode (but still within the backend type)
        # First try with the specific dialect
        MQMFactoryKey(query_proc_mode=QueryProcessingMode.basic, backend_type=backend_type, dialect=dialect),
        # If still nothing, try without specifying the dialect (all dialects within backend)
        MQMFactoryKey(query_proc_mode=QueryProcessingMode.basic, backend_type=backend_type, dialect=None),
    )

    # Now iterate over all of these combinations IN THAT VERY ORDER(!)
    factory_cls: Optional[Type[MultiQueryMutatorFactoryBase]] = None
    for key in prioritized_keys:
        factory_cls = _MQM_FACTORY_REGISTRY.get(key)
        if factory_cls is not None:
            break  # found something

    if factory_cls is None:
        # Not found for any of the combinations
        # Use the ultimate default
        factory_cls = _get_default_mqm_factory_cls()

    assert factory_cls is not None
    return factory_cls


def register_multi_query_mutator_factory_cls(
    query_proc_mode: QueryProcessingMode,
    backend_type: SourceBackendType,
    dialects: Collection[Optional[DialectCombo]],
    factory_cls: Type[MultiQueryMutatorFactoryBase],
) -> None:
    for dialect in dialects:
        key = MQMFactoryKey(query_proc_mode=query_proc_mode, backend_type=backend_type, dialect=dialect)
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


_IS_FORKABLE_DIALECT: dict[DialectName, bool] = {}
_IS_FORKABLE_DEFAULT = True


def is_forkable_dialect(dialect: DialectCombo) -> bool:
    return _IS_FORKABLE_DIALECT.get(dialect.common_name, _IS_FORKABLE_DEFAULT)


def register_forkable_dialect_name(dialect_name: DialectName, is_forkable: bool) -> None:
    try:
        assert _IS_FORKABLE_DIALECT[dialect_name] is is_forkable
    except KeyError:
        _IS_FORKABLE_DIALECT[dialect_name] = is_forkable
