from __future__ import annotations

from typing import (
    TYPE_CHECKING,
)

import attr

from dl_formula.core.dialect import StandardDialect as D
from dl_formula.definitions.scope import Scope
from dl_formula_ref.examples.config import (
    ExampleConfig,
    ExampleSource,
)
from dl_formula_ref.examples.data_table import DataTable
from dl_formula_ref.examples.dumper import (
    DataDumper,
    get_dumper,
)
from dl_formula_ref.examples.query_gen import QueryGenerator
from dl_formula_ref.examples.result_storage import WritableDataStorage
from dl_formula_ref.examples.sa_compiler import SaQueryCompiler
from dl_formula_ref.examples.utils import (
    make_data_table_from_example,
    make_key_for_example,
)


if TYPE_CHECKING:
    from dl_formula.core.dialect import DialectCombo
    from dl_formula_testing.database import Db


@attr.s
class DataPreparer:
    _db_by_dialect: dict[DialectCombo, Db] = attr.ib(kw_only=True)
    _storage_filename: str = attr.ib(kw_only=True)
    _function_scopes: int = attr.ib(kw_only=True, default=Scope.EXPLICIT_USAGE)
    _query_gen: QueryGenerator = attr.ib(kw_only=True, factory=QueryGenerator)
    _dumper_by_dialect: dict[DialectCombo, DataDumper] = attr.ib(init=False, factory=dict)
    _storage: WritableDataStorage = attr.ib(init=False)
    _sa_compiler_by_dialect: dict[DialectCombo, SaQueryCompiler] = attr.ib(init=False, factory=dict)
    _default_example_dialect: DialectCombo = attr.ib(kw_only=True, default=D.DUMMY)

    def __attrs_post_init__(self) -> None:
        self._storage = WritableDataStorage(filename=self._storage_filename)

    def _get_example_dialect(self, example: ExampleConfig) -> DialectCombo:
        if example.dialect is not None:
            return example.dialect
        return self._default_example_dialect

    def _get_db(self, dialect: DialectCombo) -> Db:
        return self._db_by_dialect[dialect]

    def _get_dumper(self, dialect: DialectCombo) -> DataDumper:
        if dialect not in self._dumper_by_dialect:
            self._dumper_by_dialect[dialect] = get_dumper(db=self._get_db(dialect=dialect))
        return self._dumper_by_dialect[dialect]

    def _get_sa_compiler(self, dialect: DialectCombo) -> SaQueryCompiler:
        if dialect not in self._sa_compiler_by_dialect:
            self._sa_compiler_by_dialect[dialect] = SaQueryCompiler(
                dialect=dialect,
                function_scopes=self._function_scopes,
            )
        return self._sa_compiler_by_dialect[dialect]

    def apply_data_transformation(self, example: ExampleConfig, data_table: DataTable) -> DataTable:
        dumper = self._get_dumper(dialect=self._get_example_dialect(example))
        sa_compiler = self._get_sa_compiler(dialect=self._get_example_dialect(example))
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
                dialect=self._get_example_dialect(example),
                order_by=example.order_by,
            )
            data_table = self.apply_data_transformation(example=example_from_result, data_table=data_table)

        self._storage.set_result_data(storage_key=storage_key, result_data=data_table)

    def save(self) -> None:
        self._storage.save()
