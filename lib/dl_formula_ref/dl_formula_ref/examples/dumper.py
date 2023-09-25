from contextlib import contextmanager
from typing import (
    Any,
    Generator,
)
import uuid

import attr
import sqlalchemy as sa
from sqlalchemy.types import TypeEngine

from dl_formula.connectors.base.type_constructor import get_type_constructor
from dl_formula.core.datatype import DataType
from dl_formula_ref.examples.data_table import (
    DataColumn,
    DataTable,
)
from dl_formula_ref.examples.query import (
    CompiledQueryContext,
    TableReference,
)
from dl_formula_testing.database import Db


def _make_name() -> str:
    return f"t_{uuid.uuid4().hex[:10]}"


@attr.s
class DataDumper:
    _db: Db = attr.ib(kw_only=True)

    def get_sa_type(self, data_type: DataType) -> TypeEngine:
        type_constructor = get_type_constructor(dialect_name=self._db.dialect.common_name)
        return type_constructor.get_sa_type(data_type)

    def generate_sa_table(self, table_ref: TableReference) -> sa.Table:
        sa_columns = [sa.Column(name=col.name, type_=self.get_sa_type(col.data_type)) for col in table_ref.columns]
        sa_table = self._db._engine_wrapper.table_from_columns(columns=sa_columns, table_name=table_ref.name)  # noqa
        return sa_table

    def create_table(self, table: DataTable) -> TableReference:
        name = _make_name()
        table_ref = TableReference(name=name, columns=table.columns)
        sa_table = self.generate_sa_table(table_ref)
        self._db.create_table(sa_table)
        return table_ref

    def dump_data(self, table_ref: TableReference, table: DataTable) -> None:
        data_as_dicts = [{col.name: value for col, value in zip(table.columns, row)} for row in table.rows]
        sa_table = self.generate_sa_table(table_ref)
        self._db.insert_into_table(sa_table, data_as_dicts)

    def remove_table(self, table_ref: TableReference) -> None:
        sa_table = self.generate_sa_table(table_ref)
        self._db.drop_table(sa_table)

    def create_and_dump_table(self, data_table: DataTable) -> TableReference:
        table_ref = self.create_table(table=data_table)
        self.dump_data(table_ref=table_ref, table=data_table)
        return table_ref

    @contextmanager
    def temporary_data_table(self, data_table: DataTable) -> Generator[TableReference, None, None]:
        table_ref = self.create_and_dump_table(data_table=data_table)
        try:
            yield table_ref
        finally:
            self.remove_table(table_ref)

    def execute_query(self, query_ctx: CompiledQueryContext) -> DataTable:
        def convert(val: Any, col: DataColumn) -> Any:
            # a workaround for missing boolean type in DBs and no coercion to bool
            if col.data_type in (DataType.CONST_BOOLEAN, DataType.BOOLEAN) and val in (0, 1):
                val = bool(val)
            return val

        rows = [
            [convert(val, col) for val, col in zip(row, query_ctx.result_columns)]
            for row in self._db.execute(query_ctx.sa_query).fetchall()
        ]
        result_data_table = DataTable(columns=list(query_ctx.result_columns), rows=rows)
        return result_data_table


def get_dumper(db: Db) -> DataDumper:
    return DataDumper(db=db)
