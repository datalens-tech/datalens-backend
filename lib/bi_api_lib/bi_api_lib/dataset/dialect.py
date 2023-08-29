from __future__ import annotations

from typing import Dict

from bi_constants.enums import SourceBackendType

from bi_formula.core.dialect import DialectName

from bi_connector_clickhouse.formula.constants import DIALECT_NAME_CLICKHOUSE


_DIALECT_NAMES_FROM_SA: Dict[SourceBackendType, DialectName] = {}


def register_dialect_name(backend_type: SourceBackendType, dialect_name: DialectName) -> None:
    if backend_type in _DIALECT_NAMES_FROM_SA:
        assert _DIALECT_NAMES_FROM_SA[backend_type] == dialect_name
    _DIALECT_NAMES_FROM_SA[backend_type] = dialect_name


def resolve_dialect_name(backend_type: SourceBackendType) -> DialectName:
    return _DIALECT_NAMES_FROM_SA.get(backend_type, DialectName.DUMMY)


# TODO: Connectorize
register_dialect_name(backend_type=SourceBackendType.CHYDB, dialect_name=DIALECT_NAME_CLICKHOUSE)
