from __future__ import annotations

from dl_constants.enums import SourceBackendType
from dl_formula.core.dialect import DialectName


_DIALECT_NAMES_FROM_SA: dict[SourceBackendType, DialectName] = {}


def register_dialect_name(backend_type: SourceBackendType, dialect_name: DialectName) -> None:
    if backend_type in _DIALECT_NAMES_FROM_SA:
        assert _DIALECT_NAMES_FROM_SA[backend_type] == dialect_name
    _DIALECT_NAMES_FROM_SA[backend_type] = dialect_name


def resolve_dialect_name(backend_type: SourceBackendType) -> DialectName:
    return _DIALECT_NAMES_FROM_SA.get(backend_type, DialectName.DUMMY)
