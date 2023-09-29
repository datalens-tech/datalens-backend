from typing import (
    Collection,
    Optional,
    Type,
)

import attr

from dl_constants.enums import (
    QueryProcessingMode,
    SourceBackendType,
)
from dl_core.fields import ResultSchema
from dl_formula.core.dialect import DialectCombo
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


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class MQMFactoryKey:
    query_proc_mode: QueryProcessingMode
    backend_type: SourceBackendType
    dialect: Optional[DialectCombo]


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class MQMFactorySettingItem:
    query_proc_mode: QueryProcessingMode
    factory_cls: Type[MultiQueryMutatorFactoryBase]
    dialects: Collection[Optional[DialectCombo]] = attr.ib(default=(None,))


_MQM_FACTORY_REGISTRY: dict[MQMFactoryKey, Type[MultiQueryMutatorFactoryBase]] = {}


def get_multi_query_mutator_factory(
    query_proc_mode: QueryProcessingMode,
    backend_type: SourceBackendType,
    dialect: DialectCombo,
    result_schema: ResultSchema,
) -> Optional[MultiQueryMutatorFactoryBase]:
    factory_cls = _MQM_FACTORY_REGISTRY.get(
        # First try with exact dialect
        MQMFactoryKey(query_proc_mode=query_proc_mode, backend_type=backend_type, dialect=dialect),
        _MQM_FACTORY_REGISTRY.get(
            # Then try without the dialect, just the backend
            MQMFactoryKey(query_proc_mode=query_proc_mode, backend_type=backend_type, dialect=None),
            DefaultMultiQueryMutatorFactory,  # If still nothing, then use the default
        ),
    )

    if factory_cls is None:
        return None

    return factory_cls(result_schema=result_schema)


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
