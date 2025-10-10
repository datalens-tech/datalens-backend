import datetime
import typing
from typing import Any

import sqlalchemy as sa
from sqlalchemy import __version__ as sa_version
from sqlalchemy.exc import CompileError
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import literal_column
from sqlalchemy.sql.compiler import SQLCompiler
from sqlalchemy.util.compat import inspect_getfullargspec
import ydb
import ydb_sqlalchemy.sqlalchemy as ydb_sa

import dl_sqlalchemy_ydb.dialect as ydb_dialect


if sa_version.startswith("2."):
    from sqlalchemy import (
        ColumnElement,
        FunctionElement,
    )
else:
    from sqlalchemy.sql.expression import (
        ColumnElement,
        FunctionElement,
    )


class YqlListType(ydb_sa.types.ListType):
    def result_processor(self, dialect: sa.engine.Dialect, coltype: typing.Any) -> typing.Any:
        def process(value: typing.Optional[list]) -> typing.Optional[list]:
            if value is None:
                return None

            if not isinstance(value, list):
                return value

            converted_value = []
            for item in value:
                if isinstance(item, bytes):
                    converted_value.append(item.decode("utf-8", errors="replace"))
                converted_value.append(item)

            return converted_value

        return process


# TODO: Try use ydb.Optional?
class YqlOptionalItemListType(YqlListType):
    """
    :class:`ListType` with optional item types
    """

    ...


class YqlTimestamp(sa.types.DateTime):
    def result_processor(self, dialect: sa.engine.Dialect, coltype: typing.Any) -> typing.Any:
        def process(value: typing.Optional[datetime.datetime]) -> typing.Optional[datetime.datetime]:
            if value is None:
                return None
            if not self.timezone:
                return value
            return value.replace(tzinfo=datetime.timezone.utc)

        return process


class YqlDateTime(YqlTimestamp, sa.types.DateTime):
    def bind_processor(self, dialect: sa.engine.Dialect) -> typing.Any:
        def process(value: typing.Optional[datetime.datetime]) -> typing.Optional[int]:
            if value is None:
                return None
            if not self.timezone:
                value = value.replace(tzinfo=datetime.timezone.utc)
            return int(value.timestamp())

        return process


class YqlInterval(sa.types.Interval):
    __visit_name__ = "interval"

    def result_processor(self, dialect: sa.engine.Dialect, coltype: typing.Any) -> typing.Any:
        def process(value: typing.Optional[datetime.timedelta]) -> typing.Optional[int]:
            if value is None:
                return None
            if isinstance(value, datetime.timedelta):
                return int(value.total_seconds() * 1_000_000)
            return value

        return process


class YqlClosure(ydb_sa.types.Lambda):
    """
    Special kind of lambda used to closure call arguments.

    Converts `lambda a, b, c: ...` into `(($a, $b, $c) -> ...) (a, b, c)`.

    Can be used to move references to columns out of lambda body.

    Example:

    Use in python:
    ```python
    Closure(
        lambda num_col, list_col: sa.func.ListFilter(
            list_col,
            lambda item: item > num_col,
        ),
        num_column,
        list_column,
    )
    ```

    Is converted into:
    ```yql
    (
        ($num_col, $list_col) -> ListFilter(
            $list_col,
            ($item) -> ($item > $num_col)
        )
    ) (num_column, list_column)
    ```

    This allows creating references to table columns inside lambda function calls by using wrapper lambda.
    """

    __visit_name__ = "closure"

    def __init__(
        self,
        func,
        *args,
    ):
        super().__init__(func)

        self.args = args


class YqlDotAccess(ColumnElement):
    """
    Special kind of sqlalchemy statement for accessing tuple's elements through dot.

    Converts expression and indes into `expression.index`, example: `tupl.0`.
    """

    __visit_name__ = "dot_access"

    def __init__(
        self,
        expression,
        index: int,
    ):
        self.dot_expression = expression
        self.dot_index = index


class YqlListLiteral(FunctionElement):
    """
    Literal generator for YDB
    """

    name = "list_literal"
    inherit_cache = True


@compiles(ydb_dialect.YqlListLiteral)
def _compile_list_literal(element: ydb_dialect.YqlListLiteral, compiler: SQLCompiler, **kw: Any) -> str:
    return compiler.process(sa.func.AsList(*element.clauses), **kw)


class CustomYqlTypeCompiler(ydb_sa.YqlTypeCompiler):
    def visit_DATETIME(self, type_: sa.DATETIME, **kw: typing.Any) -> typing.Any:
        return self.visit_datetime(type_, **kw)

    def visit_datetime(self, type_: sa.DateTime, **kw: typing.Any) -> typing.Any:
        return "DateTime"

    def visit_INTEGER(self, type_: sa.INTEGER, **kw: typing.Any) -> typing.Any:
        return self.visit_integer(type_, **kw)

    def visit_integer(self, type_: sa.Integer, **kw: typing.Any) -> typing.Any:
        return "int32"

    def visit_SMALLINT(self, type_: sa.SMALLINT, **kw: typing.Any) -> typing.Any:
        return self.visit_small_integer(type_, **kw)

    def visit_small_integer(self, type_: sa.SmallInteger, **kw: typing.Any) -> typing.Any:
        return "int16"

    def visit_BIGINT(self, type_: sa.BIGINT, **kw: typing.Any) -> typing.Any:
        return self.visit_big_integer(type_, **kw)

    def visit_big_integer(self, type_: sa.BigInteger, **kw: typing.Any) -> typing.Any:
        return "int64"

    def get_ydb_type(
        self, type_: sa.types.TypeEngine, is_optional: bool
    ) -> typing.Union[ydb_sa.ydb.PrimitiveType, ydb_sa.ydb.AbstractTypeBuilder]:
        if isinstance(type_, sa.TypeDecorator):
            type_ = type_.impl

        ydb_type = None

        # Datetime -> Datetime
        if isinstance(type_, sa.DATETIME):
            ydb_type = ydb_sa.ydb.PrimitiveType.Datetime
        if isinstance(type_, sa.DateTime):
            ydb_type = ydb_sa.ydb.PrimitiveType.Datetime
        if isinstance(type_, sa.TIMESTAMP):
            ydb_type = ydb_sa.ydb.PrimitiveType.Timestamp

        # Integer -> Int32
        if isinstance(type_, sa.Integer):
            ydb_type = ydb_sa.ydb.PrimitiveType.Int32

        # SmallInteger -> Int16
        if isinstance(type_, sa.SMALLINT):
            ydb_type = ydb_sa.ydb.PrimitiveType.Int16
        if isinstance(type_, sa.SmallInteger):
            ydb_type = ydb_sa.ydb.PrimitiveType.Int16

        # BigInteger -> Int64
        if isinstance(type_, sa.BIGINT):
            ydb_type = ydb_sa.ydb.PrimitiveType.Int64
        if isinstance(type_, sa.BigInteger):
            ydb_type = ydb_sa.ydb.PrimitiveType.Int64

        if ydb_type is not None:
            if is_optional:
                return ydb.OptionalType(ydb_type)
            return ydb_type

        return super().get_ydb_type(type_, is_optional)

    def visit_ARRAY(self, type_: sa.ARRAY, **kw: Any):
        inner = self.process(type_.item_type, **kw)

        if isinstance(type_, YqlOptionalItemListType):
            return f"List<{inner}?>"
        else:
            return f"List<{inner}>"

    def visit_list_type(self, type_: ydb_sa.types.ListType, **kw: Any):
        inner = self.process(type_.item_type, **kw)

        if isinstance(type_, YqlOptionalItemListType):
            return f"List<{inner}?>"
        else:
            return f"List<{inner}>"


class CustomYqlCompiler(ydb_sa.YqlCompiler):
    _type_compiler_cls = CustomYqlTypeCompiler

    def visit_lambda(self, lambda_: ydb_sa.types.Lambda, **kw: Any):
        func = lambda_.func
        spec = inspect_getfullargspec(func)

        if spec.varargs:
            raise CompileError("Lambdas with *args are not supported")
        if spec.varkw:
            raise CompileError("Lambdas with **kwargs are not supported")

        args = [literal_column("$" + arg) for arg in spec.args]
        text = f'({", ".join("$" + arg for arg in spec.args)}) -> ' f"{{ RETURN {self.process(func(*args), **kw)} ;}}"

        return text

    def visit_closure(self, closure: YqlClosure, **kw: Any):
        func = closure.func
        spec = inspect_getfullargspec(func)

        if spec.varargs:
            raise CompileError("Closures with *args are not supported")
        if spec.varkw:
            raise CompileError("Closures with **kwargs are not supported")

        args = [literal_column("$" + arg) for arg in spec.args]
        text = (
            f'(({", ".join("$" + arg for arg in spec.args)}) -> '
            f"{{ RETURN {self.process(func(*args), **kw)} ;}})"
            f'({", ".join(self.process(arg, **kw) for arg in closure.args)})'
        )

        return text

    def visit_dot_access(self, dot_access: YqlDotAccess, **kw):
        return f"{self.process(dot_access.dot_expression, **kw)}.{dot_access.dot_index}"


class CustomYqlDialect(ydb_sa.YqlDialect):
    type_compiler = CustomYqlTypeCompiler
    statement_compiler = CustomYqlCompiler

    colspecs = {
        **ydb_sa.YqlDialect.colspecs,
        **{
            sa.types.INTEGER: ydb_sa.types.Int32,
            sa.types.Integer: ydb_sa.types.Int32,
            sa.types.BIGINT: ydb_sa.types.Int64,
            sa.types.BigInteger: ydb_sa.types.Int64,
            sa.types.SMALLINT: ydb_sa.types.Int16,
            sa.types.SmallInteger: ydb_sa.types.Int16,
            sa.types.DateTime: YqlDateTime,
            sa.types.DATETIME: YqlDateTime,
            sa.types.TIMESTAMP: YqlTimestamp,
            sa.types.Interval: YqlInterval,
        },
    }

    def __init__(self, *args: typing.Any, **kwargs: typing.Any):
        super().__init__(
            *args,
            **{
                **kwargs,
                **dict(_add_declare_for_yql_stmt_vars=True),
            },
        )


class CustomAsyncYqlDialect(CustomYqlDialect):
    driver = "ydb_async"
    is_async = True
    supports_statement_cache = True

    def connect(self, *cargs: typing.Any, **cparams: typing.Any) -> ydb_sa.AdaptedAsyncConnection:
        return ydb_sa.AdaptedAsyncConnection(ydb_sa.util.await_only(self.dbapi.async_connect(*cargs, **cparams)))


def register_dialect() -> None:
    from sqlalchemy.dialects import registry

    registry.register("yql.ydb", __name__, "CustomYqlDialect")
    registry.register("ydb", __name__, "CustomYqlDialect")
    registry.register("yql", __name__, "CustomYqlDialect")
    registry.register("yql.ydb_async", __name__, "CustomAsyncYqlDialect")
    registry.register("ydb_async", __name__, "CustomAsyncYqlDialect")


# TODO: Refactor: move classes into separate files
