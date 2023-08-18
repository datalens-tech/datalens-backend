from __future__ import annotations

from typing import Callable, Dict, Optional

from sqlalchemy.types import TypeEngine

from bi_core.db.native_type import GenericNativeType
from bi_core.connectors.clickhouse_base.sa_types import SQLALCHEMY_CLICKHOUSE_TYPES


SQLALCHEMY_TYPES: Dict[GenericNativeType, Callable[[GenericNativeType], TypeEngine]] = {}


def make_sa_type(native_type: GenericNativeType, nullable: Optional[bool] = None) -> TypeEngine:
    if nullable is not None:
        # For a type without `nullable`, specifies it.
        native_type = native_type.as_common(default_nullable=nullable)
    type_gen = (
        SQLALCHEMY_TYPES.get(native_type) or
        SQLALCHEMY_TYPES[native_type.as_generic])
    return type_gen(native_type)


def register_sa_types(sa_type_dict: Dict[GenericNativeType, Callable[[GenericNativeType], TypeEngine]]) -> None:
    SQLALCHEMY_TYPES.update(sa_type_dict)


register_sa_types(SQLALCHEMY_CLICKHOUSE_TYPES)
