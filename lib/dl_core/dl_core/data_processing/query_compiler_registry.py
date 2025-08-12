
from dl_constants.enums import SourceBackendType
from dl_core.connectors.base.query_compiler import QueryCompiler


_QUERY_COMPILER_CLS_BY_BACKEND: dict[SourceBackendType, type[QueryCompiler]] = {}


def register_sa_query_compiler_cls(backend_type: SourceBackendType, sa_query_compiler_cls: type[QueryCompiler]) -> None:
    if (registered_compiler_cls := _QUERY_COMPILER_CLS_BY_BACKEND.get(backend_type)) is not None:
        assert registered_compiler_cls == sa_query_compiler_cls
    else:
        _QUERY_COMPILER_CLS_BY_BACKEND[backend_type] = sa_query_compiler_cls


def get_sa_query_compiler_cls(backend_type: SourceBackendType) -> type[QueryCompiler]:
    return _QUERY_COMPILER_CLS_BY_BACKEND[backend_type]
