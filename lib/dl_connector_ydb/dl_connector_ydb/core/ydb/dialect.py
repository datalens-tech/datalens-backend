import datetime
import typing

import sqlalchemy as sa
import ydb_sqlalchemy.sqlalchemy as ydb_sa


class YqlTimestamp(sa.types.DateTime):
    def result_processor(self, dialect: sa.engine.Dialect, coltype: typing.Any) -> typing.Any:
        def process(value: typing.Optional[datetime.datetime]) -> typing.Optional[datetime.datetime]:
            if value is None:
                return None
            if not self.timezone:
                return value
            return value.replace(tzinfo=datetime.timezone.utc)

        return process

    def literal_processor(self, dialect: sa.engine.Dialect) -> typing.Any:
        def process(value: datetime.datetime) -> str:
            formatted_dt = (
                value.astimezone(datetime.timezone.utc).replace(tzinfo=None).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            )
            return f'Timestamp("{ formatted_dt }")'

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

    def literal_processor(self, dialect: sa.engine.Dialect) -> typing.Any:
        def process(value: datetime.datetime) -> str:
            formatted_dt = value.astimezone(datetime.timezone.utc).replace(tzinfo=None).strftime("%Y-%m-%dT%H:%M:%SZ")
            return f'Datetime("{ formatted_dt }")'

        return process


class YqlDate(sa.types.Date, sa.types.DateTime):
    def literal_processor(self, dialect: sa.engine.Dialect) -> typing.Any:
        def process(value: datetime.date) -> str:
            formatted_dt = value.strftime("%Y-%m-%d")
            return f'Date("{ formatted_dt }")'

        return process


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

        # Datetime -> Datetime
        if isinstance(type_, sa.DATETIME):
            return ydb_sa.ydb.PrimitiveType.Datetime
        if isinstance(type_, sa.DateTime):
            return ydb_sa.ydb.PrimitiveType.Datetime
        if isinstance(type_, sa.TIMESTAMP):
            return ydb_sa.ydb.PrimitiveType.Timestamp

        # Integer -> Int32
        if isinstance(type_, sa.Integer):
            return ydb_sa.ydb.PrimitiveType.Int32

        # SmallInteger -> Int16
        if isinstance(type_, sa.SMALLINT):
            return ydb_sa.ydb.PrimitiveType.Int16
        if isinstance(type_, sa.SmallInteger):
            return ydb_sa.ydb.PrimitiveType.Int16

        # BigInteger -> Int64
        if isinstance(type_, sa.BIGINT):
            return ydb_sa.ydb.PrimitiveType.Int64
        if isinstance(type_, sa.BigInteger):
            return ydb_sa.ydb.PrimitiveType.Int64

        return super().get_ydb_type(type_, is_optional)


class CustomYqlCompiler(ydb_sa.YqlCompiler):
    _type_compiler_cls = CustomYqlTypeCompiler


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
            sa.types.Date: YqlDate,
            sa.types.DATE: YqlDate,
            sa.types.DateTime: YqlDateTime,
            sa.types.DATETIME: YqlDateTime,
            sa.types.TIMESTAMP: YqlTimestamp,
        },
    }

    def __init__(self, *args: typing.Any, **kwargs: typing.Any):
        super().__init__(
            self,
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

    registry.register("yql.ydb", "dl_connector_ydb.core.ydb.dialect", "CustomYqlDialect")
    registry.register("ydb", "dl_connector_ydb.core.ydb.dialect", "CustomYqlDialect")
    registry.register("yql", "dl_connector_ydb.core.ydb.dialect", "CustomYqlDialect")
    registry.register("yql.ydb_async", "dl_connector_ydb.core.ydb.dialect", "CustomAsyncYqlDialect")
    registry.register("ydb_async", "dl_connector_ydb.core.ydb.dialect", "CustomAsyncYqlDialect")
