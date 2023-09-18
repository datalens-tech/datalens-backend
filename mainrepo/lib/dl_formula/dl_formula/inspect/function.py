from __future__ import annotations

from functools import lru_cache
from typing import Sequence

from dl_formula.core.datatype import DataType
from dl_formula.core.dialect import DialectCombo
from dl_formula.core.dialect import StandardDialect as D
from dl_formula.definitions.registry import (
    OPERATION_REGISTRY,
    FuncKey,
)
from dl_formula.definitions.scope import Scope
from dl_formula.inspect.registry.registry import LOWLEVEL_OP_REGISTRY


@lru_cache(maxsize=1000)
def can_be_aggregate(name: str) -> bool:
    """
    Return ``True`` if the function identified by ``name`` can an aggregate function
    (there may be a window and non-window variants).
    """
    return LOWLEVEL_OP_REGISTRY.can_be_aggregate(name=name)


@lru_cache(maxsize=1000)
def can_be_window(name: str) -> bool:
    """Return ``True`` if the function identified by ``name`` can be a window function"""
    return LOWLEVEL_OP_REGISTRY.can_be_window(name=name)


@lru_cache(maxsize=1000)
def can_be_nonwindow(name: str) -> bool:
    """Return ``True`` if the function identified by ``name`` can be a non-window function"""
    return LOWLEVEL_OP_REGISTRY.can_be_nonwindow(name=name)


@lru_cache(maxsize=1000)
def supports_grouping(name: str, is_window: bool) -> bool:
    """Return ``True`` if the function identified by ``name`` supports grouping clauses"""
    return LOWLEVEL_OP_REGISTRY.supports_grouping(name=name, is_window=is_window)


@lru_cache(maxsize=1000)
def supports_ordering(name: str, is_window: bool) -> bool:
    """Return ``True`` if the function identified by ``name`` supports ``ORDER BY`` clauses"""
    return LOWLEVEL_OP_REGISTRY.supports_ordering(name=name, is_window=is_window)


@lru_cache(maxsize=1000)
def supports_lod(name: str, is_window: bool) -> bool:
    """Return ``True`` if the function identified by ``name`` supports level-of-detail (LOD) clauses"""
    return LOWLEVEL_OP_REGISTRY.supports_lod(name=name, is_window=is_window)


@lru_cache(maxsize=1000)
def supports_ignore_dimensions(name: str, is_window: bool) -> bool:
    """Return ``True`` if the function identified by ``name`` supports ``IGNORE DIMENSIONS`` clauses"""
    return LOWLEVEL_OP_REGISTRY.supports_ignore_dimensions(name=name, is_window=is_window)


@lru_cache(maxsize=1000)
def supports_bfb(name: str, is_window: bool) -> bool:
    """Return ``True`` if the function identified by ``name`` supports ``BEFORE FILTER BY`` clauses"""
    return LOWLEVEL_OP_REGISTRY.supports_bfb(name=name, is_window=is_window)


@lru_cache(maxsize=1000)
def supports_extensions(name: str, is_window: bool) -> bool:
    """Return ``True`` if the function identified by ``name`` supports any kind of extension clause"""
    return (
        supports_lod(name=name, is_window=is_window)
        or supports_grouping(name=name, is_window=is_window)
        or supports_ordering(name=name, is_window=is_window)
        or supports_bfb(name=name, is_window=is_window)
        or supports_ignore_dimensions(name=name, is_window=is_window)
    )


@lru_cache(maxsize=1000)
def requires_grouping(name: str, is_window: bool) -> bool:
    """Return ``True`` if the (window) function identified by ``name`` requires the grouping clause"""
    return is_window and can_be_nonwindow(name=name)


@lru_cache(maxsize=1000)
def uses_default_ordering(name: str) -> bool:
    """Return ``True`` if the window function identified by ``name`` uses the same ordering as the query"""
    return LOWLEVEL_OP_REGISTRY.uses_default_ordering(name=name, is_window=True)


def get_return_type(name: str, arg_types: Sequence[DataType], is_window: bool = False) -> DataType:
    """Return the data type of the function result if it is called with the given argument types"""
    # FIXME: Switch to LOWLEVEL_OP_REGISTRY
    func_definition = OPERATION_REGISTRY.get_definition(
        name=name, arg_types=arg_types, is_window=is_window, for_any_dialect=True
    )
    return func_definition.get_return_type(arg_types=list(arg_types))


def get_supported_functions(
    require_dialects: DialectCombo = D.EMPTY,
    only_functions: bool = True,
    function_scopes: int = Scope.EXPLICIT_USAGE,
) -> list[FuncKey]:
    # FIXME: Switch to LOWLEVEL_OP_REGISTRY
    return OPERATION_REGISTRY.get_supported_functions(
        require_dialects=require_dialects,
        only_functions=only_functions,
        function_scopes=function_scopes,
    )
