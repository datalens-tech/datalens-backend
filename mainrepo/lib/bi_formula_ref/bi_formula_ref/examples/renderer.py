from __future__ import annotations

import datetime
from decimal import Decimal
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Optional,
    Sequence,
    Tuple,
)

import attr
import jinja2
import tabulate

from bi_formula_ref.examples.data_table import rename_columns
from bi_formula_ref.examples.result_storage import ReadableDataStorage
from bi_formula_ref.examples.utils import (
    make_data_table_from_example,
    make_key_for_example,
)
from bi_formula_ref.i18n.registry import get_localizer

if TYPE_CHECKING:
    from bi_formula_ref.examples.config import ExampleConfig
    from bi_formula_ref.examples.data_table import DataTable


@attr.s(frozen=True)
class ExampleResult:
    name: str = attr.ib(kw_only=True)
    result: str = attr.ib(kw_only=True)
    source: Optional[str] = attr.ib(kw_only=True, default=None)
    group_by: Sequence[str] = attr.ib(kw_only=True, factory=list)
    order_by: Sequence[str] = attr.ib(kw_only=True, factory=list)
    formula_fields: Sequence[Tuple[str, str]] = attr.ib(kw_only=True, factory=list)


@attr.s
class ExampleRenderer:
    _example_template_filename: str = attr.ib(kw_only=True)
    _jinja_env: jinja2.Environment = attr.ib(kw_only=True)
    _locale: str = attr.ib(kw_only=True)
    _storage_filename: str = attr.ib(kw_only=True)
    _storage: ReadableDataStorage = attr.ib(init=False)
    _trans: Callable[[str], str] = attr.ib(init=False)

    def __attrs_post_init__(self) -> None:
        self._storage = ReadableDataStorage(filename=self._storage_filename)
        self._storage.load()
        self._trans = get_localizer(self._locale).translate

    def _format_value(self, value: Any, example: ExampleConfig) -> str:
        if value is None:
            return "NULL"
        if isinstance(value, (float, Decimal)):
            return example.float_format.format(value)
        if isinstance(value, (datetime.datetime, datetime.date)):
            return value.isoformat()
        return repr(value)

    def _md_escape(self, text: str) -> str:
        return text.replace("*", "&ast;")

    def _render_table(self, table: DataTable, example: ExampleConfig) -> str:
        return tabulate.tabulate(
            [[f"`{self._format_value(cell, example=example)}`" for cell in row] for row in table.rows],
            headers=[f"**{self._md_escape(col.name)}**" for col in table.columns],
            tablefmt="pipe",
        )

    def render_example(self, example: ExampleConfig, result_table: DataTable, under_cut: bool) -> str:
        source_table = make_data_table_from_example(example=example)

        if example.formulas_as_names:
            if example.override_formula_fields is not None:
                name_mapping = {name: formula for name, formula in example.override_formula_fields}
            elif not example.additional_transformations:
                name_mapping = {name: formula for name, formula in example.formula_fields}
            else:
                name_mapping = {name: formula for name, formula in example.additional_transformations[-1]}
            result_table = rename_columns(result_table, name_mapping=name_mapping)

        example_result = ExampleResult(
            name=example.name,
            order_by=example.order_by,
            group_by=example.group_by,
            source=self._render_table(source_table, example=example) if example.show_source_table else None,
            result=self._render_table(result_table, example=example),
            formula_fields=example.formula_fields if not example.formulas_as_names else [],
        )
        template = self._jinja_env.get_template(self._example_template_filename)
        return template.render(example_result=example_result, under_cut=under_cut, _=self._trans)

    def _get_result_table_from_storage(self, func_name: str, example: ExampleConfig) -> DataTable:
        storage_key = make_key_for_example(func_name=func_name, example=example)
        result_table = self._storage.get_result_data(storage_key)
        return result_table

    def render_example_from_storage(self, func_name: str, example: ExampleConfig, under_cut: bool) -> str:
        result_table = self._get_result_table_from_storage(func_name=func_name, example=example)
        return self.render_example(example=example, result_table=result_table, under_cut=under_cut)
