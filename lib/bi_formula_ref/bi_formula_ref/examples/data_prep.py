from __future__ import annotations

from typing import Dict, TYPE_CHECKING

import attr

from bi_formula.definitions.scope import Scope

from bi_formula_ref.examples.data_table import DataTable
from bi_formula_ref.examples.config import ExampleConfig, ExampleSource
from bi_formula_ref.examples.query_gen import QueryGenerator
from bi_formula_ref.examples.dumper import DataDumper, get_dumper
from bi_formula_ref.examples.result_storage import WritableDataStorage
from bi_formula_ref.examples.sa_compiler import SaQueryCompiler
from bi_formula_ref.examples.utils import make_key_for_example, make_data_table_from_example

if TYPE_CHECKING:
    from bi_formula.core.dialect import DialectCombo
    from bi_formula.testing.database import Db


@attr.s
class DataPreparer:
    _db_by_dialect: Dict[DialectCombo, Db] = attr.ib(kw_only=True)
    _storage_filename: str = attr.ib(kw_only=True)
    _function_scopes: int = attr.ib(kw_only=True, default=Scope.EXPLICIT_USAGE)
    _query_gen: QueryGenerator = attr.ib(kw_only=True, factory=QueryGenerator)
    _dumper_by_dialect: Dict[DialectCombo, DataDumper] = attr.ib(init=False, factory=dict)
    _storage: WritableDataStorage = attr.ib(init=False)
    _sa_compiler_by_dialect: Dict[DialectCombo, SaQueryCompiler] = attr.ib(init=False, factory=dict)

    def __attrs_post_init__(self) -> None:
        self._storage = WritableDataStorage(filename=self._storage_filename)

    def _get_db(self, dialect: DialectCombo) -> Db:
        return self._db_by_dialect[dialect]

    def _get_dumper(self, dialect: DialectCombo) -> DataDumper:
        if dialect not in self._dumper_by_dialect:
            self._dumper_by_dialect[dialect] = get_dumper(db=self._get_db(dialect=dialect))
        return self._dumper_by_dialect[dialect]

    def _get_sa_compiler(self, dialect: DialectCombo) -> SaQueryCompiler:
        if dialect not in self._sa_compiler_by_dialect:
            self._sa_compiler_by_dialect[dialect] = SaQueryCompiler(
                dialect=dialect, function_scopes=self._function_scopes,
            )
        return self._sa_compiler_by_dialect[dialect]

    def apply_data_transformation(self, example: ExampleConfig, data_table: DataTable) -> DataTable:
        dumper = self._get_dumper(dialect=example.dialect)
        sa_compiler = self._get_sa_compiler(dialect=example.dialect)
        with dumper.temporary_data_table(data_table=data_table) as table_ref:
            raw_query = self._query_gen.generate_query(example=example)
            compiled_query_ctx = sa_compiler.compile_query(raw_query=raw_query, table_ref=table_ref)
            result_data = dumper.execute_query(query_ctx=compiled_query_ctx)
        return result_data

    def generate_example_data(self, func_name: str, example: ExampleConfig) -> None:
        storage_key = make_key_for_example(func_name=func_name, example=example)
        data_table = make_data_table_from_example(example=example)
        data_table = self.apply_data_transformation(example=example, data_table=data_table)

        # Apply additional transformations
        for formula_fields in example.additional_transformations:
            source_from_result = ExampleSource(
                columns=[(col.name, col.data_type) for col in data_table.columns],
                data=data_table.rows,
            )
            example_from_result = ExampleConfig(
                source=source_from_result,
                formula_fields=formula_fields,
                dialect=example.dialect,
                order_by=example.order_by,
            )
            data_table = self.apply_data_transformation(example=example_from_result, data_table=data_table)

        self._storage.set_result_data(storage_key=storage_key, result_data=data_table)

    def save(self) -> None:
        self._storage.save()
