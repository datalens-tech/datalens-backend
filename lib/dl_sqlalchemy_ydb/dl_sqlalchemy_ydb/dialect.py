import datetime
import typing
import uuid

import sqlalchemy as sa
from sqlalchemy.engine import reflection
import ydb
import ydb_sqlalchemy.sqlalchemy as ydb_sa


class YqlTimestamp(sa.types.DateTime):
    __visit_name__ = "Timestamp"

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
            dt = value.astimezone(datetime.timezone.utc)
            dt = dt.replace(tzinfo=None)
            formatted_dt = dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

            return f'Timestamp("{ formatted_dt }")'

        return process


class YqlTimestamp64(YqlTimestamp):
    __visit_name__ = "Timestamp64"


class YqlDateTime(YqlTimestamp, sa.types.DateTime):
    __visit_name__ = "Datetime"

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
            dt = value.astimezone(datetime.timezone.utc)
            dt = dt.replace(tzinfo=None)
            formatted_dt = dt.strftime("%Y-%m-%dT%H:%M:%SZ")

            return f'Datetime("{ formatted_dt }")'

        return process


class YqlDateTime64(YqlDateTime):
    __visit_name__ = "Datetime64"


class YqlInterval(sa.types.Interval):
    __visit_name__ = "Interval"

    def result_processor(self, dialect: sa.engine.Dialect, coltype: typing.Any) -> typing.Any:
        def process(value: typing.Optional[datetime.timedelta] | int) -> typing.Optional[int]:
            if value is None:
                return None
            if isinstance(value, datetime.timedelta):
                return int(value.total_seconds() * 1_000_000)
            return value

        return process


class YqlInterval64(YqlInterval):
    __visit_name__ = "Interval64"


class YqlDate(sa.types.Date, sa.types.DateTime):
    def literal_processor(self, dialect: sa.engine.Dialect) -> typing.Any:
        def process(value: datetime.date) -> str:
            formatted_dt = value.strftime("%Y-%m-%d")
            return f'Date("{ formatted_dt }")'

        return process


class YqlString(sa.types.TEXT):
    __visit_name__ = "String"


class YqlDouble(sa.types.FLOAT):
    __visit_name__ = "Double"


class YqlFloat(sa.types.FLOAT):
    __visit_name__ = "Float"


class YqlUtf8(sa.types.TEXT):
    __visit_name__ = "Utf8"


class YqlUuid(sa.types.TEXT):
    __visit_name__ = "Uuid"

    def literal_processor(self, dialect: sa.engine.Dialect):
        def process(value: uuid.UUID | str | None) -> str | None:
            if value is None:
                return "NULL"

            if not isinstance(value, uuid.UUID):
                value = uuid.UUID(value)

            return f'UUID("{str(value)}")'

        return process

    def result_processor(self, dialect: sa.engine.Dialect, coltype: typing.Any):
        def process(value: uuid.UUID | str | None) -> uuid.UUID | None:
            if value is None:
                return None

            if isinstance(value, uuid.UUID):
                return value

            return uuid.UUID(str(value))

        return process


class CustomYqlTypeCompiler(ydb_sa.YqlTypeCompiler):
    def visit_datetime(self, type_: sa.DateTime, **kw: typing.Any) -> typing.Any:
        return "Datetime"

    def visit_DATETIME(self, type_: sa.DATETIME, **kw: typing.Any) -> typing.Any:
        return self.visit_datetime(type_, **kw)

    def visit_Datetime(self, type_: sa.DateTime, **kw: typing.Any) -> typing.Any:
        return self.visit_datetime(type_, **kw)

    def visit_datetime64(self, type_: sa.DateTime, **kw: typing.Any) -> typing.Any:
        return "Datetime64"

    def visit_DATETIME64(self, type_: sa.DATETIME, **kw: typing.Any) -> typing.Any:
        return self.visit_datetime64(type_, **kw)

    def visit_Datetime64(self, type_: sa.DateTime, **kw: typing.Any) -> typing.Any:
        return self.visit_datetime64(type_, **kw)

    def visit_timestamp(self, type_: sa.DateTime, **kw: typing.Any) -> typing.Any:
        return "Timestamp"

    def visit_TIMESTAMP(self, type_: sa.DATETIME, **kw: typing.Any) -> typing.Any:
        return self.visit_timestamp(type_, **kw)

    def visit_Timestamp(self, type_: sa.DateTime, **kw: typing.Any) -> typing.Any:
        return self.visit_timestamp(type_, **kw)

    def visit_timestamp64(self, type_: sa.DateTime, **kw: typing.Any) -> typing.Any:
        return "Timestamp64"

    def visit_TIMESTAMP64(self, type_: sa.DATETIME, **kw: typing.Any) -> typing.Any:
        return self.visit_timestamp64(type_, **kw)

    def visit_Timestamp64(self, type_: sa.DateTime, **kw: typing.Any) -> typing.Any:
        return self.visit_timestamp64(type_, **kw)

    def visit_interval(self, type_: sa.DateTime, **kw: typing.Any) -> typing.Any:
        return "Interval"

    def visit_INTERVAL(self, type_: sa.DATETIME, **kw: typing.Any) -> typing.Any:
        return self.visit_interval(type_, **kw)

    def visit_Interval(self, type_: sa.DateTime, **kw: typing.Any) -> typing.Any:
        return self.visit_interval(type_, **kw)

    def visit_interval64(self, type_: sa.DateTime, **kw: typing.Any) -> typing.Any:
        return "Interval64"

    def visit_INTERVAL64(self, type_: sa.DATETIME, **kw: typing.Any) -> typing.Any:
        return self.visit_interval64(type_, **kw)

    def visit_Interval64(self, type_: sa.DateTime, **kw: typing.Any) -> typing.Any:
        return self.visit_interval64(type_, **kw)

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

    def visit_String(self, type_: typing.Any, **kw: typing.Any) -> typing.Any:
        return "String"

    def visit_Utf8(self, type_: typing.Any, **kw: typing.Any) -> typing.Any:
        return "Utf8"

    def visit_Double(self, type_: typing.Any, **kw: typing.Any) -> typing.Any:
        return "Double"

    def visit_Float(self, type_: typing.Any, **kw: typing.Any) -> typing.Any:
        return "Float"

    def visit_Uuid(self, type_: typing.Any, **kw: typing.Any) -> typing.Any:
        return "UUID"

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


COLUMN_TYPES = {
    ydb.PrimitiveType.Int8: ydb_sa.types.Int8,
    ydb.PrimitiveType.Int16: ydb_sa.types.Int16,
    ydb.PrimitiveType.Int32: ydb_sa.types.Int32,
    ydb.PrimitiveType.Int64: ydb_sa.types.Int64,
    ydb.PrimitiveType.Uint8: ydb_sa.types.UInt8,
    ydb.PrimitiveType.Uint16: ydb_sa.types.UInt16,
    ydb.PrimitiveType.Uint32: ydb_sa.types.UInt32,
    ydb.PrimitiveType.Uint64: ydb_sa.types.UInt64,
    ydb.PrimitiveType.Float: YqlFloat,
    ydb.PrimitiveType.Double: YqlDouble,
    ydb.PrimitiveType.String: YqlString,
    ydb.PrimitiveType.Utf8: YqlUtf8,
    ydb.PrimitiveType.Json: sa.JSON,
    ydb.PrimitiveType.JsonDocument: sa.JSON,
    ydb.DecimalType: sa.DECIMAL,
    ydb.PrimitiveType.Yson: sa.TEXT,
    ydb.PrimitiveType.Date: YqlDate,
    ydb.PrimitiveType.Date32: YqlDate,
    ydb.PrimitiveType.Datetime: YqlDateTime,
    ydb.PrimitiveType.Datetime64: YqlDateTime,
    ydb.PrimitiveType.Timestamp: YqlTimestamp,
    ydb.PrimitiveType.Timestamp64: YqlTimestamp,
    ydb.PrimitiveType.Interval: YqlInterval,
    ydb.PrimitiveType.Interval64: YqlInterval64,
    ydb.PrimitiveType.Bool: sa.BOOLEAN,
    ydb.PrimitiveType.DyNumber: sa.FLOAT,
    ydb.PrimitiveType.UUID: YqlUuid,
}


def _get_column_info(t: type) -> tuple[ydb.PrimitiveType, bool]:
    nullable = False
    if isinstance(t, ydb.OptionalType):
        nullable = True
        t = t.item

    if isinstance(t, ydb.DecimalType):
        return sa.DECIMAL(precision=t.precision, scale=t.scale), nullable

    return COLUMN_TYPES[t], nullable


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

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        table = self._describe_table(connection, table_name, schema)
        as_compatible = []
        for column in table.columns:
            col_type, nullable = _get_column_info(column.type)
            as_compatible.append(
                {
                    "name": column.name,
                    "type": col_type,
                    "nullable": nullable,
                    "default": None,
                }
            )

        return as_compatible


class CustomAsyncYqlDialect(CustomYqlDialect):
    driver = "ydb_async"
    is_async = True
    supports_statement_cache = True

    def connect(self, *cargs: typing.Any, **cparams: typing.Any) -> ydb_sa.AdaptedAsyncConnection:
        return ydb_sa.AdaptedAsyncConnection(ydb_sa.util.await_only(self.dbapi.async_connect(*cargs, **cparams)))


def register_dialect() -> None:
    from sqlalchemy.dialects import registry

    registry.register("yql.ydb", "dl_sqlalchemy_ydb.dialect", "CustomYqlDialect")
    registry.register("ydb", "dl_sqlalchemy_ydb.dialect", "CustomYqlDialect")
    registry.register("yql", "dl_sqlalchemy_ydb.dialect", "CustomYqlDialect")
    registry.register("yql.ydb_async", "dl_sqlalchemy_ydb.dialect", "CustomAsyncYqlDialect")
    registry.register("ydb_async", "dl_sqlalchemy_ydb.dialect", "CustomAsyncYqlDialect")
