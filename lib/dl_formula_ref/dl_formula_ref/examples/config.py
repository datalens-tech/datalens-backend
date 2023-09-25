from __future__ import annotations

from typing import (
    Any,
    List,
    Optional,
    Sequence,
    Tuple,
)

import attr

from dl_formula.core.datatype import DataType
from dl_formula.core.dialect import DialectCombo
from dl_formula_ref.examples.data_table import DataTable
from dl_formula_ref.texts import EXAMPLE_TITLE


@attr.s(frozen=True)
class ExampleSource:
    columns: List[Tuple[str, DataType]] = attr.ib(kw_only=True)
    data: List[List[Any]] = attr.ib(kw_only=True)


@attr.s(frozen=True)
class ExampleConfig:
    source: ExampleSource = attr.ib(kw_only=True)
    # e.g. formula_fields = (('My Field', 'STR([col1])'),)
    formula_fields: List[Tuple[str, str]] = attr.ib(kw_only=True)
    dialect: Optional[DialectCombo] = attr.ib(kw_only=True, default=None)
    name: str = attr.ib(kw_only=True, default=EXAMPLE_TITLE)
    # group_by and order_by are lists of formulas
    group_by: Sequence[str] = attr.ib(kw_only=True, default=())
    order_by: Sequence[str] = attr.ib(kw_only=True, default=())
    show_source_table: bool = attr.ib(kw_only=True, default=False)
    formulas_as_names: bool = attr.ib(kw_only=True, default=True)
    float_format: str = attr.ib(kw_only=True, default="{:.02f}")
    # Sequence of transformations described by formulas just like in `formula_fields`
    # They are applied to the result table in the same sequence as they are defined.
    additional_transformations: Sequence[List[Tuple[str, str]]] = attr.ib(kw_only=True, default=())
    # If additional transformations are applied,
    # then one might want to disguise real formulas used for the result with these overrides
    override_formula_fields: Optional[List[Tuple[str, str]]] = attr.ib(kw_only=True, default=None)


@attr.s(frozen=True)
class PredefinedExampleConfig(ExampleConfig):
    result_table: DataTable = attr.ib(kw_only=True)
