import abc
import contextlib
import datetime
import re
from typing import (
    Any,
    ClassVar,
    Generator,
    Optional,
    Sequence,
    Type,
)
import uuid

import pytest
import pytz
import sqlalchemy as sa
from sqlalchemy.types import TypeEngine

from dl_db_testing.database.base import DbBase
from dl_db_testing.database.dispenser import DbDispenserBase
from dl_db_testing.database.engine_wrapper import (
    DbEngineConfig,
    TableSpec,
)
from dl_formula.core.datatype import DataType
from dl_formula.core.dialect import DialectCombo
from dl_formula_testing.database import (
    FormulaDbConfig,
    FormulaDbDispenser,
)
from dl_formula_testing.evaluator import DbEvaluator
from dl_formula_testing.table import (
    NULL_DATA_TABLE_SPEC,
    TABLE_SPEC,
    TABLE_SPEC_ARRAYS,
    ColumnSpec,
    generate_sample_data,
    get_column_sa_type,
)


class FormulaConnectorTestBase(metaclass=abc.ABCMeta):
    dialect: ClassVar[DialectCombo]
    eval_attempts: ClassVar[int] = 1
    retry_on_exceptions: ClassVar[tuple[tuple[Type[Exception], re.Pattern], ...]] = ()

    supports_arrays: ClassVar[bool]
    supports_uuid: ClassVar[bool]
    bool_is_expression: ClassVar[bool] = False
    empty_str_is_null: ClassVar[bool] = False
    null_casts_to_number: ClassVar[bool] = False
    null_casts_to_false: ClassVar[bool] = False
    db_dispenser: ClassVar[DbDispenserBase] = FormulaDbDispenser()
    engine_config_cls: ClassVar[Type[DbEngineConfig]] = DbEngineConfig

    @abc.abstractmethod
    @pytest.fixture(scope="class")
    def db_url(self) -> str:
        pass

    @pytest.fixture(scope="class")
    def engine_params(self) -> dict:
        return {}

    @pytest.fixture(scope="class")
    def tzinfo(self) -> datetime.tzinfo:
        return pytz.UTC

    @pytest.fixture(scope="class")
    def engine_config(self, db_url: str, engine_params: dict) -> DbEngineConfig:
        return self.engine_config_cls(url=db_url, engine_params=engine_params)

    @pytest.fixture(scope="class")
    def db_config(self, engine_config: DbEngineConfig, tzinfo: datetime.tzinfo) -> FormulaDbConfig:
        return FormulaDbConfig(
            engine_config=engine_config,
            dialect=self.dialect,
            tzinfo=tzinfo,
        )

    @pytest.fixture(scope="class")
    def dbe(self, db_config: FormulaDbConfig) -> DbEvaluator:
        db = self.db_dispenser.get_database(db_config)
        dbe = DbEvaluator(
            db=db,
            attempts=self.eval_attempts,
            retry_on_exceptions=self.retry_on_exceptions,
        )
        return dbe

    def lowlevel_make_sa_table(
        self, db: DbBase, table_schema_name: Optional[str], table_spec: TableSpec, columns: Sequence[sa.Column]
    ) -> sa.Table:
        table = db.table_from_columns(table_name=table_spec.table_name, schema=table_schema_name, columns=columns)
        return table

    def get_column_sa_type(self, data_type: DataType) -> TypeEngine:
        return get_column_sa_type(data_type=data_type, dialect=self.dialect)

    def make_columns(self, column_specs: Sequence[ColumnSpec]) -> list[sa.Column]:
        columns = [
            sa.Column(name=spec.name, type_=self.get_column_sa_type(data_type=spec.data_type)) for spec in column_specs
        ]
        return columns

    def generate_table_name(self, prefix: str) -> str:
        return f"{prefix}_{uuid.uuid4().hex[:6]}"

    def generate_table_spec(self, table_name_prefix: str) -> TableSpec:
        return TableSpec(
            table_name=self.generate_table_name(prefix=table_name_prefix),
        )

    def make_sa_table(self, db: DbBase, table_spec: TableSpec, table_schema_name: Optional[str]) -> sa.Table:
        column_specs = list(TABLE_SPEC)
        if self.supports_arrays:
            column_specs += TABLE_SPEC_ARRAYS

        columns = self.make_columns(column_specs)
        return self.lowlevel_make_sa_table(
            db=db, table_spec=table_spec, table_schema_name=table_schema_name, columns=columns
        )

    @pytest.fixture(scope="class")
    def table_schema_name(self) -> Optional[str]:
        return None

    @pytest.fixture(scope="class")
    def data_table(self, dbe: DbEvaluator, table_schema_name: Optional[str]) -> Generator[sa.Table, None, None]:
        with self.make_data_table(dbe=dbe, table_schema_name=table_schema_name) as table:
            yield table

    @contextlib.contextmanager
    def make_data_table(self, dbe: DbEvaluator, table_schema_name: Optional[str]) -> Generator[sa.Table, None, None]:
        db = dbe.db
        table_spec = self.generate_table_spec(table_name_prefix="test_table")
        table = self.make_sa_table(db=dbe.db, table_spec=table_spec, table_schema_name=table_schema_name)
        db.create_table(table)

        table_data = generate_sample_data(add_arrays=self.supports_arrays)
        db.insert_into_table(table, table_data)

        table.int_values = [row["int_value"] for row in table_data]  # type: ignore  # 2024-01-29 # TODO: "Table" has no attribute "int_values"  [attr-defined]
        table.date_values = [row["date_value"] for row in table_data]  # type: ignore  # 2024-01-29 # TODO: "Table" has no attribute "date_values"  [attr-defined]
        table.datetime_values = [row["datetime_value"] for row in table_data]  # type: ignore  # 2024-01-29 # TODO: "Table" has no attribute "datetime_values"  [attr-defined]

        try:
            yield table
        finally:
            dbe.db.drop_table(table)

    @pytest.fixture(scope="class")
    def null_data_table(self, dbe: DbEvaluator, table_schema_name: Optional[str]) -> Generator[sa.Table, None, None]:
        with self.make_null_data_table(dbe=dbe, table_schema_name=table_schema_name) as table:
            yield table

    @contextlib.contextmanager
    def make_null_data_table(
        self, dbe: DbEvaluator, table_schema_name: Optional[str]
    ) -> Generator[sa.Table, None, None]:
        column_specs = NULL_DATA_TABLE_SPEC
        table_spec = self.generate_table_spec("null_test_table")

        columns = self.make_columns(column_specs)
        table = self.lowlevel_make_sa_table(
            db=dbe.db, table_spec=table_spec, table_schema_name=table_schema_name, columns=columns
        )
        table_data = [{column.name: None for column in column_specs}]

        dbe.db.create_table(table)
        dbe.db.insert_into_table(table, table_data)

        try:
            yield table
        finally:
            dbe.db.drop_table(table)

    @contextlib.contextmanager
    def make_scalar_table(
        self, dbe: DbEvaluator, table_schema_name: Optional[str], col_name: str, data_type: DataType, value: Any
    ) -> Generator[sa.Table, None, None]:
        table_spec = self.generate_table_spec(table_name_prefix="scalar_test_table")
        column_specs = [ColumnSpec(name=col_name, data_type=data_type)]
        columns = self.make_columns(column_specs)
        table = self.lowlevel_make_sa_table(
            db=dbe.db, table_spec=table_spec, table_schema_name=table_schema_name, columns=columns
        )
        table_data = [{col_name: value}]
        dbe.db.create_table(table)
        dbe.db.insert_into_table(table, table_data)

        try:
            yield table
        finally:
            dbe.db.drop_table(table)
