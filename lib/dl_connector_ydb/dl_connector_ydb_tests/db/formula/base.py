import contextlib
import datetime
from typing import (
    Generator,
    Optional,
    Union,
)

from frozendict import frozendict
import pytest
import sqlalchemy as sa
from sqlalchemy.sql.elements import ClauseElement

from dl_core_testing.database import Db
from dl_formula.core.datatype import DataType
from dl_formula.core.nodes import Formula
from dl_formula.definitions.scope import Scope
from dl_formula_testing.database import (
    FormulaDbConfig,
    FormulaDbDispenser,
)
from dl_formula_testing.evaluator import (
    FIELD_TYPES,
    DbEvaluator,
)
from dl_formula_testing.testcases.base import FormulaConnectorTestBase

from dl_connector_ydb.formula.constants import YqlDialect as D
import dl_connector_ydb_tests.db.config as test_config
from dl_connector_ydb_tests.db.config import DB_CONFIGURATIONS


class YqlDbDispenser(FormulaDbDispenser):
    def ensure_db_is_up(self, db: Db) -> tuple[bool, str]:
        # first, check that the db is up
        status, msg = super().ensure_db_is_up(db)
        if not status:
            return status, msg

        test_table = db.table_from_columns([sa.Column(name="col1", type_=sa.Integer())])

        # secondly, try to create a test table: for some reason
        # it could be that YDB is up but you still can't do it
        try:
            db.create_table(test_table)
            db.drop_table(test_table)
            return True, ""
        except Exception as exc:
            return False, str(exc)


class YQLDbEvaluator(DbEvaluator):
    def eval(  # type: ignore  # 2024-01-29 # TODO: Function is missing a return type annotation  [no-untyped-def]
        self,
        formula: Union[str, Formula],
        from_: Optional[ClauseElement] = None,
        where: str | Formula | None = None,
        many: bool = False,
        other_fields: Optional[dict] = None,
        order_by: Optional[list[str | Formula]] = None,
        group_by: Optional[list[str | Formula]] = None,
        first: bool = False,
        required_scopes: int = Scope.EXPLICIT_USAGE,
        field_types: Optional[dict[str, DataType]] = None,
    ):
        result = super().eval(
            formula=formula,
            from_=from_,
            where=where,
            many=many,
            other_fields=other_fields,
            order_by=order_by,
            group_by=group_by,
            first=first,
            required_scopes=required_scopes,
            field_types=field_types,
        )

        if isinstance(result, list):
            return [value.decode("utf-8", errors="replace") if isinstance(value, bytes) else value for value in result]

        return result


class YQLTestBase(FormulaConnectorTestBase):
    dialect = D.YDB
    supports_arrays = False
    supports_uuid = True
    db_dispenser = YqlDbDispenser()

    # YDB-specific field types for formula testing
    YDB_FIELD_TYPES = {
        "integer_value": DataType.INTEGER,
        "timestamp_value": DataType.DATETIME,  # YDB TIMESTAMP maps to DATETIME in formula system
        "datetime_value": DataType.DATETIME,
    }

    @pytest.fixture(scope="class")
    def db_url(self) -> str:
        return DB_CONFIGURATIONS[self.dialect]

    @pytest.fixture(scope="class")
    def ydb_data_table(self, dbe: DbEvaluator, table_schema_name: Optional[str]) -> Generator[sa.Table, None, None]:
        with self.make_ydb_data_table(dbe=dbe, table_schema_name=table_schema_name) as table:
            yield table

    @contextlib.contextmanager
    def make_ydb_data_table(
        self, dbe: DbEvaluator, table_schema_name: Optional[str]
    ) -> Generator[sa.Table, None, None]:
        db = dbe.db
        table_spec = self.generate_table_spec(table_name_prefix="ydb_test_table")

        columns = [
            sa.Column("integer_value", sa.Integer(), primary_key=True),
            sa.Column("timestamp_value", sa.TIMESTAMP()),
            sa.Column("datetime_value", sa.DateTime()),
        ]

        table = self.lowlevel_make_sa_table(
            db=db, table_spec=table_spec, table_schema_name=table_schema_name, columns=columns
        )

        db.create_table(table)

        base_dt = datetime.datetime(2000, 1, 2, 3, 4, 5)
        table_data = [
            {
                "integer_value": 1,
                "timestamp_value": base_dt,
                "datetime_value": base_dt,
            },
            {
                "integer_value": 2,
                "timestamp_value": base_dt + datetime.timedelta(hours=1),
                "datetime_value": base_dt + datetime.timedelta(hours=1),
            },
            {
                "integer_value": 3,
                "timestamp_value": base_dt + datetime.timedelta(days=1),
                "datetime_value": base_dt + datetime.timedelta(days=1),
            },
            {
                "integer_value": 4,
                "timestamp_value": base_dt + datetime.timedelta(weeks=42),
                "datetime_value": base_dt + datetime.timedelta(weeks=42),
            },
            {
                "integer_value": 5,
                "timestamp_value": base_dt + datetime.timedelta(microseconds=123456890),
                "datetime_value": base_dt + datetime.timedelta(microseconds=123456890),
            },
        ]

        db.insert_into_table(table, table_data)

        table.timestamp_values = [row["timestamp_value"] for row in table_data]  # type: ignore
        table.datetime_values = [row["datetime_value"] for row in table_data]  # type: ignore
        table.integer_values = [row["integer_value"] for row in table_data]  # type: ignore

        try:
            yield table
        finally:
            dbe.db.drop_table(table)

    @pytest.fixture(scope="function")
    def ydb_data_table_field_types(self) -> dict[str, DataType]:
        return {**FIELD_TYPES, **self.YDB_FIELD_TYPES}

    @pytest.fixture(scope="class")
    def engine_params(self) -> dict:
        return dict(
            connect_args=frozendict(
                dict(
                    host=test_config.CoreConnectionSettings.HOST,
                    port=test_config.CoreConnectionSettings.PORT,
                    protocol="grpc",
                )
            ),
        )

    @pytest.fixture(scope="class")
    def dbe(self, db_config: FormulaDbConfig) -> DbEvaluator:
        db = self.db_dispenser.get_database(db_config)
        dbe = YQLDbEvaluator(
            db=db,
            attempts=self.eval_attempts,
            retry_on_exceptions=self.retry_on_exceptions,
        )
        return dbe
