import contextlib
import datetime
from typing import (
    Generator,
    Optional,
)

import pytest
import sqlalchemy as sa

from dl_formula.core.datatype import DataType
import dl_formula.core.exc as exc
from dl_formula_testing.evaluator import DbEvaluator
from dl_formula_testing.testcases.functions_type_conversion import (
    DefaultBoolTypeFunctionFormulaConnectorTestSuite,
    DefaultDateTypeFunctionFormulaConnectorTestSuite,
    DefaultDbCastTypeFunctionFormulaConnectorTestSuite,
    DefaultFloatTypeFunctionFormulaConnectorTestSuite,
    DefaultGenericDatetimeTypeFunctionFormulaConnectorTestSuite,
    DefaultGeopointTypeFunctionFormulaConnectorTestSuite,
    DefaultGeopolygonTypeFunctionFormulaConnectorTestSuite,
    DefaultIntTypeFunctionFormulaConnectorTestSuite,
    DefaultStrTypeFunctionFormulaConnectorTestSuite,
)
from dl_formula_testing.util import to_str
import dl_sqlalchemy_ydb.dialect as ydb_dialect

from dl_connector_ydb_tests.db.formula.base import YQLTestBase


# STR


class TestStrTypeFunctionYQL(YQLTestBase, DefaultStrTypeFunctionFormulaConnectorTestSuite):
    zero_float_to_str_value = "0"
    skip_custom_tz = True

    def test_str_from_datetime(self, dbe: DbEvaluator) -> None:
        assert to_str(dbe.eval("STR(#2019-01-02 03:04:05#)")) == "2019-01-02T03:04:05Z"


# FLOAT


class TestFloatTypeFunctionYQL(YQLTestBase, DefaultFloatTypeFunctionFormulaConnectorTestSuite):
    pass


# BOOL


class TestBoolTypeFunctionYQL(YQLTestBase, DefaultBoolTypeFunctionFormulaConnectorTestSuite):
    pass


# INT


class TestIntTypeFunctionYQL(YQLTestBase, DefaultIntTypeFunctionFormulaConnectorTestSuite):
    pass


# DATE


class TestDateTypeFunctionYQL(YQLTestBase, DefaultDateTypeFunctionFormulaConnectorTestSuite):
    pass


# GENERICDATETIME (& DATETIME)


class TestGenericDatetimeTypeFunctionYQL(
    YQLTestBase,
    DefaultGenericDatetimeTypeFunctionFormulaConnectorTestSuite,
):
    pass


# GEOPOINT


class TestGeopointTypeFunctionYQL(YQLTestBase, DefaultGeopointTypeFunctionFormulaConnectorTestSuite):
    pass


# GEOPOLYGON


class TestGeopolygonTypeFunctionYQL(YQLTestBase, DefaultGeopolygonTypeFunctionFormulaConnectorTestSuite):
    pass


# DB_CAST


class DbCastTypeFunctionYQLTestSuite(
    DefaultDbCastTypeFunctionFormulaConnectorTestSuite,
):
    def test_db_cast_ydb(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        # Valid cast
        value = dbe.eval("[int_value]", from_=data_table)
        assert dbe.eval('DB_CAST(FLOAT([int_value]), "Double")', from_=data_table) == pytest.approx(float(value))

        # # Test that it works with bool
        dbe.eval('DB_CAST(BOOL([int_value]), "Double")', from_=data_table)
        # Test that it works with int
        dbe.eval('DB_CAST(INT([int_value]), "Int64")', from_=data_table)
        # Test that it works with float
        dbe.eval('DB_CAST(FLOAT([int_value]), "Double")', from_=data_table)
        # Test that it works with string
        dbe.eval('DB_CAST(STR([int_value]), "Utf8")', from_=data_table)
        # Test that it works with uuid
        dbe.eval('DB_CAST(STR("00000000-0000-0000-0000-000000000000"), "Uuid")', from_=data_table)
        dbe.eval('DB_CAST(STR("8098116c-07e7-4670-8c1d-dea21848b031"), "Uuid")', from_=data_table)

        # Cast to decimal with correct arguments
        assert dbe.eval('DB_CAST([int_value], "Decimal", 5, 1)', from_=data_table) == value

        # Invalid number of arguments for Decimal
        with pytest.raises(exc.TranslationError):
            dbe.eval('DB_CAST([int_value], "Decimal", 5)', from_=data_table)

        with pytest.raises(exc.TranslationError):
            dbe.eval('DB_CAST([int_value], "Decimal", "5", "3")', from_=data_table)

        # Invalid cast from Integer to Uuid
        with pytest.raises(exc.TranslationError):
            dbe.eval('DB_CAST([int_value], "Uuid")', from_=data_table)

        # Cast into itself
        assert dbe.eval('DB_CAST(DB_CAST([int_value], "Int64"), "Int64")', from_=data_table) == value

        # Cast and cast back
        assert dbe.eval('DB_CAST(DB_CAST(DB_CAST([int_value], "Int64"), "UInt64"), "Int64")', from_=data_table) == value

        # Castn't
        with pytest.raises(exc.TranslationError):
            assert dbe.eval('DB_CAST([int_value], "meow")', from_=data_table) == value

    def _test_db_cast_ydb_func(
        self,
        dbe: DbEvaluator,
        ydb_type_test_data_table: sa.Table,
        target: str,
        cast_args: tuple[int, int] | None,
        ok: bool,
        ydb_data_test_table_field_types,
        source_column: str,
    ) -> None:
        if cast_args:
            cast_args_str = ", ".join(str(arg) for arg in cast_args)
            query_string = f'DB_CAST([{source_column}], "{target}", {cast_args_str})'
        else:
            query_string = f'DB_CAST([{source_column}], "{target}")'

        if ok:
            dbe.eval(query_string, from_=ydb_type_test_data_table, field_types=ydb_data_test_table_field_types)
        else:
            with pytest.raises(exc.TranslationError):
                dbe.eval(query_string, from_=ydb_type_test_data_table, field_types=ydb_data_test_table_field_types)

    @pytest.mark.parametrize(
        # target - target type for cast
        # cast_args - type arguments (for decimal)
        # ok - if no exception should occur
        "target,cast_args,ok",
        [
            # Bool
            ("Bool", None, True),
            # Int
            ("Int8", None, True),
            ("Int16", None, True),
            ("Int32", None, True),
            ("Int64", None, True),
            ("UInt8", None, True),
            ("UInt16", None, True),
            ("UInt32", None, True),
            # Float
            ("Float", None, True),
            ("Double", None, True),
            # String
            ("String", None, True),
            ("Utf8", None, False),
            # Date
            ("Date", None, False),
            ("Datetime", None, False),
            ("Timestamp", None, False),
            # Uuid
            ("Uuid", None, False),
        ],
    )
    def test_db_cast_ydb_bool(
        self,
        dbe: DbEvaluator,
        ydb_type_test_data_table: sa.Table,
        target: str,
        cast_args: tuple[int, int] | None,
        ok: bool,
        ydb_data_test_table_field_types,
    ) -> None:
        self._test_db_cast_ydb_func(
            dbe=dbe,
            ydb_type_test_data_table=ydb_type_test_data_table,
            target=target,
            cast_args=cast_args,
            ok=ok,
            ydb_data_test_table_field_types=ydb_data_test_table_field_types,
            source_column="bool_value",
        )

    @pytest.mark.parametrize(
        # target - target type for cast
        # cast_args - type arguments (for decimal)
        # ok - if no exception should occur
        "target,cast_args,ok",
        [
            # Bool
            ("Bool", None, True),
            # Int
            ("Int8", None, True),
            ("Int16", None, True),
            ("Int32", None, True),
            ("Int64", None, True),
            ("UInt8", None, True),
            ("UInt16", None, True),
            ("UInt32", None, True),
            # Float
            ("Float", None, True),
            ("Double", None, True),
            # String
            ("String", None, True),
            ("Utf8", None, False),
            # Date
            ("Date", None, True),
            ("Datetime", None, True),
            ("Timestamp", None, True),
            # Uuid
            ("Uuid", None, False),
        ],
    )
    def test_db_cast_ydb_integer(
        self,
        dbe: DbEvaluator,
        ydb_type_test_data_table: sa.Table,
        target: str,
        cast_args: tuple[int, int] | None,
        ok: bool,
        ydb_data_test_table_field_types,
    ) -> None:
        self._test_db_cast_ydb_func(
            dbe=dbe,
            ydb_type_test_data_table=ydb_type_test_data_table,
            target=target,
            cast_args=cast_args,
            ok=ok,
            ydb_data_test_table_field_types=ydb_data_test_table_field_types,
            source_column="int64_value",
        )

    @pytest.mark.parametrize(
        # target - target type for cast
        # cast_args - type arguments (for decimal)
        # ok - if no exception should occur
        "target,cast_args,ok",
        [
            # Bool
            ("Bool", None, True),
            # Int
            ("Int8", None, True),
            ("Int16", None, True),
            ("Int32", None, True),
            ("Int64", None, True),
            ("UInt8", None, True),
            ("UInt16", None, True),
            ("UInt32", None, True),
            # Float
            ("Float", None, True),
            ("Double", None, True),
            # String
            ("String", None, True),
            ("Utf8", None, False),
            # Date
            ("Date", None, False),
            ("Datetime", None, False),
            ("Timestamp", None, False),
            # Uuid
            ("Uuid", None, False),
        ],
    )
    def test_db_cast_ydb_float(
        self,
        dbe: DbEvaluator,
        ydb_type_test_data_table: sa.Table,
        target: str,
        cast_args: tuple[int, int] | None,
        ok: bool,
        ydb_data_test_table_field_types,
    ) -> None:
        self._test_db_cast_ydb_func(
            dbe=dbe,
            ydb_type_test_data_table=ydb_type_test_data_table,
            target=target,
            cast_args=cast_args,
            ok=ok,
            ydb_data_test_table_field_types=ydb_data_test_table_field_types,
            source_column="float_value",
        )

    @pytest.mark.parametrize(
        # target - target type for cast
        # cast_args - type arguments (for decimal)
        # ok - if no exception should occur
        "target,cast_args,ok",
        [
            # Bool
            ("Bool", None, True),
            # Int
            ("Int8", None, True),
            ("Int16", None, True),
            ("Int32", None, True),
            ("Int64", None, True),
            ("UInt8", None, True),
            ("UInt16", None, True),
            ("UInt32", None, True),
            # Float
            ("Float", None, True),
            ("Double", None, True),
            # String
            ("String", None, True),
            ("Utf8", None, True),
            # Date
            ("Date", None, True),
            ("Datetime", None, True),
            ("Timestamp", None, True),
            # Uuid
            ("Uuid", None, True),
        ],
    )
    def test_db_cast_ydb_string(
        self,
        dbe: DbEvaluator,
        ydb_type_test_data_table: sa.Table,
        target: str,
        cast_args: tuple[int, int] | None,
        ok: bool,
        ydb_data_test_table_field_types,
    ) -> None:
        self._test_db_cast_ydb_func(
            dbe=dbe,
            ydb_type_test_data_table=ydb_type_test_data_table,
            target=target,
            cast_args=cast_args,
            ok=ok,
            ydb_data_test_table_field_types=ydb_data_test_table_field_types,
            source_column="string_value",
        )

    @pytest.mark.parametrize(
        # target - target type for cast
        # cast_args - type arguments (for decimal)
        # ok - if no exception should occur
        "target,cast_args,ok",
        [
            # Bool
            ("Bool", None, False),
            # Int
            ("Int8", None, True),
            ("Int16", None, True),
            ("Int32", None, True),
            ("Int64", None, True),
            ("UInt8", None, True),
            ("UInt16", None, True),
            ("UInt32", None, True),
            # Float
            ("Float", None, True),
            ("Double", None, True),
            # String
            ("String", None, True),
            ("Utf8", None, True),
            # Date
            ("Date", None, True),
            ("Datetime", None, True),
            ("Timestamp", None, True),
            # Uuid
            ("Uuid", None, False),
        ],
    )
    def test_db_cast_ydb_date(
        self,
        dbe: DbEvaluator,
        ydb_type_test_data_table: sa.Table,
        target: str,
        cast_args: tuple[int, int] | None,
        ok: bool,
        ydb_data_test_table_field_types,
    ) -> None:
        self._test_db_cast_ydb_func(
            dbe=dbe,
            ydb_type_test_data_table=ydb_type_test_data_table,
            target=target,
            cast_args=cast_args,
            ok=ok,
            ydb_data_test_table_field_types=ydb_data_test_table_field_types,
            source_column="date_value",
        )


class DbCastYQLTestSuiteBase(YQLTestBase):
    @contextlib.contextmanager
    def make_ydb_type_test_data_table(
        self, dbe: DbEvaluator, table_schema_name: Optional[str]
    ) -> Generator[sa.Table, None, None]:
        db = dbe.db
        table_spec = self.generate_table_spec(table_name_prefix="ydb_type_test_table")

        columns = [
            sa.Column("bool_value", sa.Boolean()),
            sa.Column("int64_value", sa.Integer(), primary_key=True),
            sa.Column("float_value", ydb_dialect.YqlFloat()),
            sa.Column("string_value", ydb_dialect.YqlString()),
            sa.Column("date_value", sa.Date()),
        ]

        table = self.lowlevel_make_sa_table(
            db=db, table_spec=table_spec, table_schema_name=table_schema_name, columns=columns
        )

        db.create_table(table)

        table_data = [
            {
                "bool_value": True,
                "int64_value": 42,
                "float_value": 0.1 + 0.2,
                "string_value": "lobster",
                "date_value": datetime.date(2000, 1, 2),
            },
        ]

        db.insert_into_table(table, table_data)

        try:
            yield table
        finally:
            dbe.db.drop_table(table)

    @pytest.fixture(scope="class")
    def ydb_type_test_data_table(
        self, dbe: DbEvaluator, table_schema_name: Optional[str]
    ) -> Generator[sa.Table, None, None]:
        with self.make_ydb_type_test_data_table(dbe=dbe, table_schema_name=table_schema_name) as table:
            yield table

    # YDB-specific field types for formula testing
    YDB_TYPE_FIELD_TYPES = {
        "bool_value": DataType.BOOLEAN,
        "int64_value": DataType.INTEGER,
        "float_value": DataType.FLOAT,
        "string_value": DataType.STRING,
        "timestamp_value": DataType.DATETIME,  # YDB TIMESTAMP maps to DATETIME in formula system
        "date_value": DataType.DATE,
        "datetime_value": DataType.DATETIME,
    }

    @pytest.fixture(scope="function")
    def ydb_data_test_table_field_types(self) -> dict[str, DataType]:
        return {**self.YDB_TYPE_FIELD_TYPES}


class TestDbCastTypeFunctionYQL(
    DbCastYQLTestSuiteBase,
    DbCastTypeFunctionYQLTestSuite,
):
    pass
