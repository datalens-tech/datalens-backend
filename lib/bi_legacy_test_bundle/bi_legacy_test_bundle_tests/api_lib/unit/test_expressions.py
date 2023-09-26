from __future__ import annotations

from typing import (
    Dict,
    List,
    Optional,
    Tuple,
)
import uuid

from clickhouse_sqlalchemy.drivers.native.base import ClickHouseDialect
import pytest

from dl_connector_clickhouse.core.clickhouse_base.constants import CONNECTION_TYPE_CLICKHOUSE
from dl_connector_clickhouse.formula.constants import DIALECT_NAME_CLICKHOUSE
from dl_connector_postgresql.formula.constants import PostgreSQLDialect
from dl_constants.enums import (
    AggregationFunction,
    BinaryJoinOperator,
    FieldType,
    UserDataType,
    WhereClauseOperation,
)
from dl_core.db import SchemaColumn
from dl_core.db.native_type import ClickHouseNativeType
from dl_core.fields import (
    BIField,
    DirectCalculationSpec,
    FormulaCalculationSpec,
)
from dl_core.multisource import (
    AvatarRelation,
    BinaryCondition,
    ConditionPartDirect,
)
from dl_core.query.expression import ExpressionCtx
from dl_formula.core.dialect import from_name_and_version
import dl_formula.core.exc as formula_exc
from dl_formula.core.message_ctx import FormulaErrorCtx
from dl_formula.definitions.scope import Scope
from dl_formula.inspect.env import InspectionEnvironment
from dl_formula.parser.factory import get_parser
from dl_query_processing.column_registry import ColumnRegistry
from dl_query_processing.compilation.filter_compiler import MainFilterFormulaCompiler
from dl_query_processing.compilation.formula_compiler import FormulaCompiler
from dl_query_processing.compilation.primitives import (
    AvatarFromObject,
    CompiledFormulaInfo,
    CompiledMultiQuery,
    CompiledQuery,
    FromColumn,
    JoinedFromObject,
)
from dl_query_processing.compilation.specs import FilterFieldSpec
from dl_query_processing.enums import ExecutionLevel
from dl_query_processing.translation.flat_translator import FlatQueryTranslator
from dl_query_processing.translation.multi_level_translator import MultiLevelQueryTranslator


def new_id() -> str:
    return str(uuid.uuid4())


_TYPE_NAME_TO_USER_TYPE = {
    "Int32": UserDataType.integer,
    "String": UserDataType.string,
    "Date": UserDataType.date,
    "Datetime": UserDataType.genericdatetime,
}


def _make_ch_column(name: str, type_name: str, source_id: str) -> SchemaColumn:
    return SchemaColumn(
        name=name,
        user_type=_TYPE_NAME_TO_USER_TYPE[type_name],
        native_type=ClickHouseNativeType.normalize_name_and_create(
            conn_type=CONNECTION_TYPE_CLICKHOUSE,
            name=type_name,
        ),
        source_id=source_id,
    )


DB_VERSION = "18.14.11"


class CompilerTestBase:
    columns: Tuple[Tuple[str, str], ...] = ()  # `((field_title, field_type_name), ...)`, e.g. `(('f3', 'String'),)`
    group_by: Tuple[str, ...] = ()  # `(field_title, ...)`, e.g. ('f3',)

    # Filled in `prepare`
    # TODO: wrap in properties that check `is not None` on read.
    is_prepared = False
    avatar_id = "ava_1"
    avatar_sid = None  # shortened
    source_id = "src_1"
    column_objs = None
    field_objs = None
    field_guids = None
    dialect = None
    formula_compiler = None
    flat_query_translator = None
    multi_query_translator = None

    def _make_columns(self):
        return [
            _make_ch_column(
                name=name,
                type_name=type_name,
                source_id=self.source_id,
            )
            for name, type_name in self.columns
        ]

    def _make_field(self, title, *, formula="", field_id=None, avatar_id=None, source=None, **kwargs):
        if formula:
            calc_spec = FormulaCalculationSpec(formula=formula)
        else:
            calc_spec = DirectCalculationSpec(avatar_id=avatar_id or self.avatar_id, source=source)
        return BIField.make(guid=field_id or new_id(), title=title, calc_spec=calc_spec, **kwargs)

    def _make_fields(self):
        return [self._make_field(col.name, type=FieldType.DIMENSION) for col in self.column_objs]

    def _make_avatar_source_map(self):
        return {self.avatar_id: self.source_id}

    def prepare(self):
        avatar_id = new_id()
        self.avatar_id = avatar_id
        self.avatar_sid = avatar_id  # separate from avatar_id in case a mapper is used
        source_id = new_id()
        self.source_id = source_id
        columns = self._make_columns()
        self.column_objs = columns
        fields = self._make_fields()
        self.field_objs = fields
        field_guids = {field.title: field.guid for field in fields}
        self.field_guids = field_guids

        self.dialect = ClickHouseDialect()

        inspect_env = InspectionEnvironment()
        formula_parser = get_parser()

        column_reg = ColumnRegistry(
            db_columns=columns,
            avatar_source_map=self._make_avatar_source_map(),
        )
        self.formula_compiler = FormulaCompiler(
            columns=column_reg,
            formula_parser=formula_parser,
            all_fields=fields,
            group_by_ids=[field_guids[name] for name in self.group_by],
            inspect_env=inspect_env,
        )
        dialect = from_name_and_version(
            dialect_name=DIALECT_NAME_CLICKHOUSE,
            dialect_version=DB_VERSION,
        )
        self.multi_query_translator = MultiLevelQueryTranslator(
            source_db_columns=column_reg,
            inspect_env=inspect_env,
            function_scopes=Scope.EXPLICIT_USAGE,
            verbose_logging=True,
            dialect=dialect,
            compeng_dialect=PostgreSQLDialect.COMPENG,
        )
        self.flat_query_translator = FlatQueryTranslator(
            columns=column_reg,
            inspect_env=inspect_env,
            function_scopes=Scope.EXPLICIT_USAGE,
            dialect=dialect,
            avatar_alias_mapper=lambda s: s,
        )
        self.is_prepared = True

    @pytest.fixture
    def prepared(self):
        self.prepare()
        return self

    def _prepare_formula_as_query(self, comp_formula: CompiledFormulaInfo) -> CompiledMultiQuery:
        joined_from = JoinedFromObject(
            froms=[
                AvatarFromObject(
                    id=self.avatar_id,
                    alias=self.avatar_id,
                    avatar_id=self.avatar_id,
                    source_id=self.source_id,
                    columns=tuple(FromColumn(id=col[0], name=col[0]) for col in self.columns),
                ),
            ],
        )
        compiled_multi_query = CompiledMultiQuery(
            queries=[
                CompiledQuery(
                    id="1",
                    joined_from=joined_from,
                    select=[comp_formula],
                    group_by=[],
                    order_by=[],
                    filters=[],
                    join_on=[],
                    limit=None,
                    offset=None,
                    level_type=ExecutionLevel.source_db,
                )
            ],
        )
        return compiled_multi_query

    def get_field_errors(self, field: BIField) -> List[FormulaErrorCtx]:
        errors = self.formula_compiler.get_field_errors(field=field)
        try:
            formula_info = self.formula_compiler.compile_field_formula(field=field, collect_errors=True)
            compiled_multi_query = self._prepare_formula_as_query(comp_formula=formula_info)
            errors += self.multi_query_translator.collect_errors(compiled_multi_query=compiled_multi_query)
        except formula_exc.FormulaError:
            pass
        return errors

    def get_formula_errors(self, formula: str) -> List[FormulaErrorCtx]:
        errors = self.formula_compiler.get_formula_errors(formula=formula)
        try:
            formula_info = self.formula_compiler.compile_text_formula(formula=formula, collect_errors=True)
            compiled_multi_query = self._prepare_formula_as_query(comp_formula=formula_info)
            errors += self.multi_query_translator.collect_errors(compiled_multi_query=compiled_multi_query)
        except formula_exc.FormulaError:
            pass
        return errors

    def make_expressions(self):
        return {
            field.title: self.flat_query_translator.translate_formula(
                comp_formula=self.formula_compiler.compile_field_formula(field),
            )
            for field in self.field_objs
        }

    def make_compiled_exprs(self):
        return {title: self.compile(expr) for title, expr in self.make_expressions().items()}

    def compile(self, expr_ctx: ExpressionCtx) -> str:
        try:
            return expr_ctx.expression.compile(
                compile_kwargs={"literal_binds": True},
                dialect=self.dialect,
            ).string
        except NotImplementedError:
            return str(expr_ctx.expression)

    def compile_filter_expression(self, field_id: str, operation: WhereClauseOperation, args: list) -> str:
        filter_compiler = MainFilterFormulaCompiler(formula_compiler=self.formula_compiler)
        comp_formula = filter_compiler.compile_filter_formula(
            filter_spec=FilterFieldSpec(field_id=field_id, operation=operation, values=args),
        )
        expr_ctx = self.flat_query_translator.translate_formula(comp_formula=comp_formula)
        return self.compile(expr_ctx)


class TestGenerateExpressions(CompilerTestBase):
    columns = (("f1", "Int32"),)
    group_by = ("f1",)

    def _make_fields(self):
        return [
            self._make_field("f1"),
            self._make_field("f2", formula="[f1] + 1"),
            self._make_field("f3", formula="[f1] + 1", aggregation=AggregationFunction.avg),
            self._make_field("f4", formula="sum([f1])"),
            self._make_field("f5", source="f1", aggregation=AggregationFunction.avg),
        ]

    def test_generate_expressions(self, prepared):
        exprs = self.make_compiled_exprs()
        assert exprs["f1"] == f'"{self.avatar_sid}".f1'
        assert exprs["f2"] in (
            f'"{self.avatar_sid}".f1 + 1',
            f'("{self.avatar_sid}".f1 + 1)',
        )  # legacy version adds parentheses
        assert exprs["f3"] in (f'avg("{self.avatar_sid}".f1 + 1)', f'avg(("{self.avatar_sid}".f1 + 1))')
        assert exprs["f4"] in (f'sum("{self.avatar_sid}".f1)', f'(sum("{self.avatar_sid}".f1))')
        assert exprs["f5"] == f'avg("{self.avatar_sid}".f1)'


class TestCasts(CompilerTestBase):
    columns = (("f1", "String"),)
    group_by = ("f1",)

    def _make_fields(self):
        return [
            self._make_field("f1"),
            self._make_field("f2", source="f1", cast=UserDataType.integer),
            self._make_field("f3", source="f1", cast=UserDataType.integer, aggregation=AggregationFunction.avg),
            self._make_field("f4", formula="[f1]", cast=UserDataType.integer),
            self._make_field("f5", formula="[f1]", cast=UserDataType.integer, aggregation=AggregationFunction.avg),
            self._make_field("f6", formula="[f2]", cast=UserDataType.string),
        ]

    def test_casts(self, prepared):
        exprs = self.make_compiled_exprs()
        assert exprs["f2"] == f'toInt64OrNull("{self.avatar_sid}".f1)'
        assert exprs["f3"] == f'avg(toInt64OrNull("{self.avatar_sid}".f1))'
        assert exprs["f4"] == f'toInt64OrNull("{self.avatar_sid}".f1)'
        assert exprs["f5"] == f'avg(toInt64OrNull("{self.avatar_sid}".f1))'
        assert exprs["f6"] == f'toString(toInt64OrNull("{self.avatar_sid}".f1))'


class TestNull(CompilerTestBase):
    def _make_fields(self):
        return [self._make_field("f1", formula="NULL")]

    def test_null_expression(self, prepared):
        exprs = self.make_compiled_exprs()
        assert exprs["f1"] == "NULL"


class TestGetSchemaErrors(CompilerTestBase):
    columns = (
        ("f1", "Int32"),
        ("f2", "Int32"),
    )

    def _make_fields(self):
        return [
            self._make_field("f1", source="f1"),
            self._make_field("f2", formula="max([f1])"),
            self._make_field("f3", formula="[f1] + 1"),
            self._make_field("f4", formula="sum([f2]) + [f2]"),
            self._make_field("f5", formula="[f1] + [f2]", aggregation=AggregationFunction.sum),
            self._make_field("f6", formula="[f1] + "),
            self._make_field("f7", formula="sum([f2]) + [f2] + [f1]"),
        ]

    def test_get_schema_errors(self, prepared):
        fields = {field.title: field for field in self.field_objs}
        assert not self.get_field_errors(fields["f1"])
        assert not self.get_field_errors(fields["f2"])
        assert not self.get_field_errors(fields["f3"])

        errors = self.get_field_errors(fields["f4"])
        assert len(errors) == 0  # Double aggregations are allowed now

        errors = self.get_field_errors(fields["f5"])
        assert len(errors) == 1
        assert errors[0].code == formula_exc.InconsistentAggregationError.default_code

        errors = self.get_field_errors(fields["f6"])
        assert len(errors) == 1
        assert errors[0].code == formula_exc.ParseUnexpectedEOFError.default_code

        errors = self.get_field_errors(fields["f7"])
        assert len(errors) == 1
        assert errors[0].code == formula_exc.InconsistentAggregationError.default_code


class TestGetFormulaErrors(CompilerTestBase):
    columns = (
        ("f1", "Int32"),
        ("f2", "Int32"),
        ("f3", "Int32"),
    )

    def _make_fields(self):
        return [
            self._make_field("f1", source="f1", type=FieldType.DIMENSION),
            self._make_field("f2", formula="SUM([f1])", type=FieldType.MEASURE),
            self._make_field("f3", source="f3", type=FieldType.DIMENSION),
        ]

    def test_get_formula_errors(self, prepared):
        assert not self.get_formula_errors("[f1]")
        assert not self.get_formula_errors("[f2]")
        assert not self.get_formula_errors("sum([f1])")

        errors = self.get_formula_errors("[f1] +")  # invalid syntax
        assert len(errors) == 1
        errors = self.get_formula_errors("unknown_func([f1])")  # unknown function
        assert len(errors) == 1
        errors = self.get_formula_errors("[f3] + sum([f3])")  # inconsistent aggregation (without substitution)
        assert len(errors) == 1
        errors = self.get_formula_errors("sum([f2])")  # double aggregation via substitution (of aggregated field f2)
        assert len(errors) == 0  # No longer an error


class TestGenerateFilterExpressionIntCol(CompilerTestBase):
    columns = (("f1", "Int32"),)

    def test_generate_filter_expression(self, prepared):
        f1 = self.field_objs[0]

        # unary
        assert (
            self.compile_filter_expression(
                f1.guid,
                WhereClauseOperation.ISNULL,
                [],
            )
            == f'isNull("{self.avatar_sid}".f1)'
        )
        assert (
            self.compile_filter_expression(
                f1.guid,
                WhereClauseOperation.ISNOTNULL,
                [],
            )
            == f'NOT isNull("{self.avatar_sid}".f1)'
        )

        # binary
        assert (
            self.compile_filter_expression(
                f1.guid,
                WhereClauseOperation.EQ,
                [12],
            )
            == f'"{self.avatar_sid}".f1 = 12'
        )
        assert (
            self.compile_filter_expression(
                f1.guid,
                WhereClauseOperation.NE,
                [12],
            )
            == f'"{self.avatar_sid}".f1 != 12'
        )
        assert (
            self.compile_filter_expression(
                f1.guid,
                WhereClauseOperation.LT,
                [12],
            )
            == f'"{self.avatar_sid}".f1 < 12'
        )
        assert (
            self.compile_filter_expression(
                f1.guid,
                WhereClauseOperation.LTE,
                [12],
            )
            == f'"{self.avatar_sid}".f1 <= 12'
        )
        assert (
            self.compile_filter_expression(
                f1.guid,
                WhereClauseOperation.GT,
                [12],
            )
            == f'"{self.avatar_sid}".f1 > 12'
        )

        # binary with list
        assert (
            self.compile_filter_expression(
                f1.guid,
                WhereClauseOperation.IN,
                [12, 34, 56],
            )
            == f'"{self.avatar_sid}".f1 IN (12, 34, 56)'
        )
        assert (
            self.compile_filter_expression(
                f1.guid,
                WhereClauseOperation.NIN,
                [12, 34, 56],
            )
            == f'("{self.avatar_sid}".f1 NOT IN (12, 34, 56)) OR "{self.avatar_sid}".f1 IS NULL'
        )
        assert (
            self.compile_filter_expression(
                f1.guid,
                WhereClauseOperation.IN,
                [12, 34, 56, None],
            )
            == f'"{self.avatar_sid}".f1 IS NULL OR "{self.avatar_sid}".f1 IN (12, 34, 56)'
        )
        assert (
            self.compile_filter_expression(
                f1.guid,
                WhereClauseOperation.NIN,
                [12, 34, 56, None],
            )
            == f'("{self.avatar_sid}".f1 NOT IN (12, 34, 56)) AND "{self.avatar_sid}".f1 IS NOT NULL'
        )
        assert (
            self.compile_filter_expression(
                f1.guid,
                WhereClauseOperation.IN,
                [None],
            )
            == f'"{self.avatar_sid}".f1 IS NULL'
        )
        assert (
            self.compile_filter_expression(
                f1.guid,
                WhereClauseOperation.NIN,
                [None],
            )
            == f'"{self.avatar_sid}".f1 IS NOT NULL'
        )

        # ternary
        assert (
            self.compile_filter_expression(
                f1.guid,
                WhereClauseOperation.BETWEEN,
                [12, 34],
            )
            == f'"{self.avatar_sid}".f1 BETWEEN 12 AND 34'
        )


class TestGenerateFilterExpressionStrCol(CompilerTestBase):
    columns = (
        ("f3", "String"),
        ("f4", "Int32"),
    )

    def test_generate_filter_expression(self, prepared):
        # Pure string
        f3 = self.field_objs[0]

        assert (
            self.compile_filter_expression(
                f3.guid,
                WhereClauseOperation.STARTSWITH,
                ["qwe"],
            )
            == f"startsWith(\"{self.avatar_sid}\".f3, 'qwe')"
        )
        assert (
            self.compile_filter_expression(
                f3.guid,
                WhereClauseOperation.ISTARTSWITH,
                ["qwe"],
            )
            == f"startsWith(lowerUTF8(\"{self.avatar_sid}\".f3), 'qwe')"
        )
        assert (
            self.compile_filter_expression(
                f3.guid,
                WhereClauseOperation.ENDSWITH,
                ["qwe"],
            )
            == f"endsWith(\"{self.avatar_sid}\".f3, 'qwe')"
        )
        assert (
            self.compile_filter_expression(
                f3.guid,
                WhereClauseOperation.IENDSWITH,
                ["qwe"],
            )
            == f"endsWith(lowerUTF8(\"{self.avatar_sid}\".f3), 'qwe')"
        )
        assert (
            self.compile_filter_expression(
                f3.guid,
                WhereClauseOperation.CONTAINS,
                ["qwe"],
            )
            == f"\"{self.avatar_sid}\".f3 LIKE '%%qwe%%'"
        )
        assert (
            self.compile_filter_expression(
                f3.guid,
                WhereClauseOperation.ICONTAINS,
                ["qwe"],
            )
            == f"positionCaseInsensitiveUTF8(\"{self.avatar_sid}\".f3, 'qwe') != 0"
        )
        assert (
            self.compile_filter_expression(
                f3.guid,
                WhereClauseOperation.NOTCONTAINS,
                ["qwe"],
            )
            == f"\"{self.avatar_sid}\".f3 NOT LIKE '%%qwe%%'"
        )
        assert (
            self.compile_filter_expression(
                f3.guid,
                WhereClauseOperation.NOTICONTAINS,
                ["qwe"],
            )
            == f"positionCaseInsensitiveUTF8(\"{self.avatar_sid}\".f3, 'qwe') = 0"
        )

        # Non-string
        f4 = self.field_objs[1]

        assert (
            self.compile_filter_expression(
                f4.guid,
                WhereClauseOperation.STARTSWITH,
                ["123"],
            )
            == f"startsWith(toString(\"{self.avatar_sid}\".f4), '123')"
        )
        assert (
            self.compile_filter_expression(
                f4.guid,
                WhereClauseOperation.ISTARTSWITH,
                ["123"],
            )
            == f"startsWith(lowerUTF8(toString(\"{self.avatar_sid}\".f4)), '123')"
        )
        assert (
            self.compile_filter_expression(
                f4.guid,
                WhereClauseOperation.ENDSWITH,
                ["123"],
            )
            == f"endsWith(toString(\"{self.avatar_sid}\".f4), '123')"
        )
        assert (
            self.compile_filter_expression(
                f4.guid,
                WhereClauseOperation.IENDSWITH,
                ["123"],
            )
            == f"endsWith(lowerUTF8(toString(\"{self.avatar_sid}\".f4)), '123')"
        )
        assert (
            self.compile_filter_expression(
                f4.guid,
                WhereClauseOperation.CONTAINS,
                ["123"],
            )
            == f"toString(\"{self.avatar_sid}\".f4) LIKE '%%123%%'"
        )
        assert (
            self.compile_filter_expression(
                f4.guid,
                WhereClauseOperation.ICONTAINS,
                ["123"],
            )
            == f"positionCaseInsensitiveUTF8(toString(\"{self.avatar_sid}\".f4), '123') != 0"
        )
        assert (
            self.compile_filter_expression(
                f4.guid,
                WhereClauseOperation.NOTCONTAINS,
                ["123"],
            )
            == f"toString(\"{self.avatar_sid}\".f4) NOT LIKE '%%123%%'"
        )
        assert (
            self.compile_filter_expression(
                f4.guid,
                WhereClauseOperation.NOTICONTAINS,
                ["123"],
            )
            == f"positionCaseInsensitiveUTF8(toString(\"{self.avatar_sid}\".f4), '123') = 0"
        )


class TestGenerateFilterExpressionDateCol(CompilerTestBase):
    columns = (("f4", "Date"),)

    def test_generate_filter_expression(self, prepared):
        f4 = self.field_objs[0]

        expr = self.compile_filter_expression(f4.guid, WhereClauseOperation.GT, ["2017-01-01T00:00:00"])
        expected = f""""{self.avatar_sid}".f4 > toDate('2017-01-01')"""
        assert expr == expected, "date column should be filtered by a date const"

        expr = self.compile_filter_expression(f4.guid, WhereClauseOperation.GT, ["2017-01-01T00:00:00+03:00"])
        expected = f""""{self.avatar_sid}".f4 > toDate('2017-01-01')"""
        assert expr == expected, "utc offset for date filtering should be ignored"

        expr = self.compile_filter_expression(f4.guid, WhereClauseOperation.GTE, ["2017-01-01T00:00:01"])
        expected = f""""{self.avatar_sid}".f4 > toDate('2017-01-01')"""
        # another_expected = f'''"{self.avatar_sid}".f4 >= toDate('2017-01-02')'''
        assert expr == expected, "gte -> gt mangle expected"

        expr = self.compile_filter_expression(
            f4.guid, WhereClauseOperation.BETWEEN, ["2017-01-01T00:00:00", "2017-01-01T23:59:59"]
        )
        expected = f""""{self.avatar_sid}".f4 BETWEEN toDate('2017-01-01') AND toDate('2017-01-01')"""
        assert expr == expected, "between-filter rounding"


class TestGenerateFilterExpressionDatetimeCol(CompilerTestBase):
    columns = (("f4", "Datetime"),)

    def test_generate_filter_expression(self, prepared):
        f4 = self.field_objs[0]

        expr = self.compile_filter_expression(f4.guid, WhereClauseOperation.GT, ["2017-01-01T00:00:00"])
        expected = f""""{self.avatar_sid}".f4 > toDateTime('2017-01-01T00:00:00')"""
        assert expr == expected, "datetime column should be filtered by a datetime const"

        expr = self.compile_filter_expression(
            f4.guid, WhereClauseOperation.BETWEEN, ["2017-01-01T00:00:00Z", "2017-01-01T23:59:59Z"]
        )
        expected = (
            f""""{self.avatar_sid}".f4 BETWEEN toDateTime('2017-01-01T00:00:00') AND """
            f"""toDateTime('2017-01-01T23:59:59')"""
        )
        assert (
            expr == expected
        ), "zulu timezone should be ignored when dealing with genericdatetime"  # at least according to Gadzhi


class TwoSourcesCompilerTestBase(CompilerTestBase):
    second_source_columns = ()

    second_source_id = None
    second_avatar_id = None
    second_avatar_sid = None

    def _make_second_source_columns(self):
        assert self.second_source_id
        return [
            _make_ch_column(
                name=name,
                type_name=type_name,
                source_id=self.second_source_id,
            )
            for name, type_name in self.second_source_columns
        ]

    def _make_columns(self):
        return list(super()._make_columns()) + list(self._make_second_source_columns())

    def _make_avatar_source_map(self):
        return {**super()._make_avatar_source_map(), self.second_avatar_id: self.second_source_id}

    def prepare(self):
        self.second_source_id = new_id()
        self.second_avatar_id = new_id()
        self.second_avatar_sid = self.second_avatar_id  # in case a mapper is used
        return super().prepare()


class TestGenerateRelationExpression(TwoSourcesCompilerTestBase):
    columns = (
        ("f1", "Int32"),
        ("f2", "Int32"),
    )
    second_source_columns = (
        ("f1", "Int32"),
        ("f3", "Int32"),
    )

    def _make_fields(self):
        return [
            self._make_field("f1", type=FieldType.DIMENSION),
            self._make_field("f2", type=FieldType.DIMENSION),
            self._make_field("f1_1", source="f1", avatar_id=self.second_avatar_id, type=FieldType.DIMENSION),
            self._make_field("f3", avatar_id=self.second_avatar_id, type=FieldType.DIMENSION),
        ]

    def test_generate_relation_expression(self, prepared):
        relation = AvatarRelation(
            id=new_id(),
            left_avatar_id=self.avatar_id,
            right_avatar_id=self.second_avatar_id,
            conditions=[
                BinaryCondition(
                    left_part=ConditionPartDirect(source="f1"),
                    right_part=ConditionPartDirect(source="f1"),
                    operator=BinaryJoinOperator.eq,
                ),
                BinaryCondition(
                    left_part=ConditionPartDirect(source="f2"),
                    right_part=ConditionPartDirect(source="f3"),
                    operator=BinaryJoinOperator.lte,
                ),
            ],
        )
        relation_formula_info = self.formula_compiler.compile_relation_formula(relation=relation)
        relation_expr_info = self.flat_query_translator.translate_formula(
            comp_formula=relation_formula_info,
        )
        relation_expr = self.compile(relation_expr_info)
        assert relation_expr == (
            f'"{self.avatar_sid}".f1 = "{self.second_avatar_sid}".f1 AND '
            f'"{self.avatar_sid}".f2 <= "{self.second_avatar_sid}".f3'
        )


class TestDoubleAggregation(CompilerTestBase):
    columns = (("f1", "Int32"),)

    def _make_fields(self):
        return [
            # [f1] The base field
            self._make_field("f1", source="f1"),
            # [f2] The base field with explicit aggregation
            self._make_field("f2", source="f1", aggregation=AggregationFunction.sum),
            # [f3] [f1] (base field) with an explicit aggregation
            self._make_field("f3", formula="SUM([f1])"),
            # [f4] [f1] (base field) with a formula (auto) aggregation
            self._make_field("f4", formula="[f1]", aggregation=AggregationFunction.sum),
            # [f5] Formula using [f2] (base field with aggregation)
            self._make_field("f5", formula="[f2]"),
            # Secondary aggregations over the above
            # [f2_2agg] [f2] + additional formula agg
            self._make_field("f2_2agg", formula="AVG([f2])"),
            # [f2_twice_2agg] Two usages of [f2] + additional formula agg for one of them
            self._make_field("f2_twice_2agg", formula="[f2] + AVG([f2])"),
            # [f3_2agg] [f3] + additional formula agg
            self._make_field("f3_2agg", formula="AVG([f3])"),
            # [f4_2agg] [f4] + additional formula agg
            self._make_field("f4_2agg", formula="AVG([f4])"),
            # [f5_2agg] [f5] + additional formula agg
            self._make_field("f5_2agg", formula="AVG([f5])"),
            # [f4_2agg_lod] [f4] + additional formula agg with LOD
            self._make_field("f4_2agg_lod", formula="AVG([f4] INCLUDE)"),
            # [f5_2agg_lod] [f5] + additional formula agg with LOD
            self._make_field("f5_2agg_lod", formula="AVG([f5] INCLUDE)"),
        ]

    def test_double_aggregation(self, prepared):
        # f1, f2, f3, f4, f5, f2_2agg, f2_twice_2agg, f3_2agg, f4_2agg, f5_2agg = fields

        expr_infos: Dict[str, Optional[ExpressionCtx]] = {}
        for field in self.field_objs:
            expr_ctx: Optional[ExpressionCtx] = None
            try:
                compiled_formula = self.formula_compiler.compile_field_formula(field)
                expr_ctx = self.flat_query_translator.translate_formula(
                    comp_formula=compiled_formula,
                    collect_errors=True,
                )
            except formula_exc.FormulaError:
                pass
            expr_infos[field.title] = expr_ctx

        exprs = {
            title: self.compile(expr_info) if expr_info is not None else None for title, expr_info in expr_infos.items()
        }

        # [f2_2agg] -> should ignore the first aggregation
        assert exprs["f2_2agg"] == f'avg("{self.avatar_sid}".f1)'

        # [f2_twice_2agg] -> should ignore the first aggregation
        assert exprs["f2_twice_2agg"] == f'sum("{self.avatar_sid}".f1) + avg("{self.avatar_sid}".f1)'

        # [f3_2agg] -> first aggregation is auto, so it cannot be ignored -> two aggregations
        assert exprs["f3_2agg"] == f'avg(sum("{self.avatar_sid}".f1))'

        # [f4_2agg] -> should ignore the first aggregation
        assert exprs["f4_2agg"] == f'avg("{self.avatar_sid}".f1)'

        # [f5_2agg] -> first aggregation is nested inside a formula -> two aggregations
        assert exprs["f5_2agg"] == f'avg(sum("{self.avatar_sid}".f1))'

        # [f4_2agg] -> should ignore the first aggregation even with LOD
        assert exprs["f4_2agg_lod"] == f'avg("{self.avatar_sid}".f1)'

        # [f5_2agg] -> first aggregation is nested inside a formula -> two aggregations
        assert exprs["f5_2agg_lod"] == f'avg(sum("{self.avatar_sid}".f1))'
