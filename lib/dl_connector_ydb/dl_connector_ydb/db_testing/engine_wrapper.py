from __future__ import annotations

import os
from typing import (
    Any,
    Callable,
    NamedTuple,
    Optional,
    Sequence,
)

import shortuuid
import sqlalchemy as sa
from sqlalchemy.types import TypeEngine
import ydb
import ydb_sqlalchemy.sqlalchemy as ydb_sa

from dl_db_testing.database.engine_wrapper import EngineWrapperBase
import dl_sqlalchemy_ydb.dialect


class YdbTypeSpec(NamedTuple):
    type: ydb.PrimitiveType
    to_sql_str: Callable[[Any], str]


_DATETIME_TYPE_SPEC = YdbTypeSpec(
    ydb.PrimitiveType.Datetime,
    to_sql_str=lambda x: f'DateTime::MakeDatetime($datetime_parse("{x}"))',
)


_TIMESTAMP_TYPE_SPEC = YdbTypeSpec(
    ydb.PrimitiveType.Timestamp,
    to_sql_str=lambda x: f'DateTime::MakeTimestamp($datetime_parse("{x}"))',
)


SA_TYPE_TO_YDB_TYPE: dict[type[TypeEngine], YdbTypeSpec] = {
    ydb_sa.types.Int8: YdbTypeSpec(type=ydb.PrimitiveType.Int8, to_sql_str=str),
    ydb_sa.types.Int16: YdbTypeSpec(type=ydb.PrimitiveType.Int16, to_sql_str=str),
    ydb_sa.types.Int32: YdbTypeSpec(type=ydb.PrimitiveType.Int32, to_sql_str=str),
    ydb_sa.types.Int64: YdbTypeSpec(type=ydb.PrimitiveType.Int64, to_sql_str=str),
    ydb_sa.types.UInt8: YdbTypeSpec(type=ydb.PrimitiveType.Uint8, to_sql_str=str),
    ydb_sa.types.UInt16: YdbTypeSpec(type=ydb.PrimitiveType.Uint16, to_sql_str=str),
    ydb_sa.types.UInt32: YdbTypeSpec(type=ydb.PrimitiveType.Uint32, to_sql_str=str),
    ydb_sa.types.UInt64: YdbTypeSpec(type=ydb.PrimitiveType.Uint64, to_sql_str=str),
    sa.SmallInteger: YdbTypeSpec(type=ydb.PrimitiveType.Uint8, to_sql_str=str),
    sa.Integer: YdbTypeSpec(type=ydb.PrimitiveType.Int32, to_sql_str=str),
    sa.BigInteger: YdbTypeSpec(type=ydb.PrimitiveType.Int64, to_sql_str=str),
    sa.Float: YdbTypeSpec(type=ydb.PrimitiveType.Double, to_sql_str=str),
    sa.Boolean: YdbTypeSpec(type=ydb.PrimitiveType.Bool, to_sql_str=lambda x: str(bool(x))),
    sa.String: YdbTypeSpec(type=ydb.PrimitiveType.String, to_sql_str=lambda x: f'"{x}"'),
    sa.BINARY: YdbTypeSpec(type=ydb.PrimitiveType.String, to_sql_str=lambda x: f'"{x}"'),
    sa.Text: YdbTypeSpec(type=ydb.PrimitiveType.String, to_sql_str=lambda x: f'"{x}"'),
    sa.Unicode: YdbTypeSpec(type=ydb.PrimitiveType.Utf8, to_sql_str=lambda x: f'"{x}"'),
    dl_sqlalchemy_ydb.dialect.YqlFloat: YdbTypeSpec(type=ydb.PrimitiveType.Float, to_sql_str=str),
    dl_sqlalchemy_ydb.dialect.YqlDouble: YdbTypeSpec(type=ydb.PrimitiveType.Double, to_sql_str=str),
    dl_sqlalchemy_ydb.dialect.YqlString: YdbTypeSpec(type=ydb.PrimitiveType.String, to_sql_str=lambda x: f'"{x}"'),
    dl_sqlalchemy_ydb.dialect.YqlUtf8: YdbTypeSpec(type=ydb.PrimitiveType.Utf8, to_sql_str=lambda x: f'"{x}"'),
    dl_sqlalchemy_ydb.dialect.YqlUuid: YdbTypeSpec(type=ydb.PrimitiveType.UUID, to_sql_str=lambda x: f'UUID("{x}")'),
    sa.Date: YdbTypeSpec(type=ydb.PrimitiveType.Date, to_sql_str=lambda x: f'DateTime::MakeDate($date_parse("{x}"))'),
    # Datetime
    # TODO: 2025.01.29: Not adding Datetime64 because YDB Datetime::Parse64 does not work
    ydb_sa.types.YqlDateTime: _DATETIME_TYPE_SPEC,
    sa.DateTime: _DATETIME_TYPE_SPEC,
    sa.DATETIME: _DATETIME_TYPE_SPEC,
    # Timestamp
    # TODO: 2025.01.29: Not adding Timestamp64 because YDB Datetime::Parse64 does not work
    ydb_sa.types.YqlTimestamp: _TIMESTAMP_TYPE_SPEC,
    sa.TIMESTAMP: _TIMESTAMP_TYPE_SPEC,
    # Interval
    dl_sqlalchemy_ydb.dialect.YqlInterval: YdbTypeSpec(
        ydb.PrimitiveType.Interval, to_sql_str=lambda x: f"CAST({x} as Interval)"
    ),
    # Interval64
    dl_sqlalchemy_ydb.dialect.YqlInterval64: YdbTypeSpec(
        ydb.PrimitiveType.Interval64, to_sql_str=lambda x: f"CAST({x} as Interval64)"
    ),
}


class YQLEngineWrapper(EngineWrapperBase):
    URL_PREFIX = "yql"

    def get_conn_credentials(self, full: bool = False) -> dict:
        return dict(
            endpoint=self.engine.url.query["endpoint"],
            db_name=self.engine.url.query["database"],
        )

    def get_version(self) -> Optional[str]:
        return None

    def _generate_table_description(self, columns: Sequence[sa.Column]) -> ydb.TableDescription:
        table = ydb.TableDescription().with_columns(
            *[ydb.Column(col.name, ydb.OptionalType(SA_TYPE_TO_YDB_TYPE[type(col.type)].type)) for col in columns]
        )
        primary_keys = [col.name for col in columns if False]  # if primary_key]  # FIXME
        if not primary_keys:
            primary_keys = [columns[0].name]
        return table.with_primary_keys(*primary_keys)

    def _get_table_path(self, table: sa.Table) -> str:
        return os.path.join(self.engine.url.query["database"], table.name)  # type: ignore  # 2024-01-24 # TODO: Argument 1 to "join" has incompatible type "str | tuple[str, ...]"; expected "str"  [arg-type]

    def _get_connection_params(self) -> ydb.DriverConfig:
        return ydb.DriverConfig(
            endpoint=self.engine.url.query["endpoint"],
            database=self.engine.url.query["database"],
        )

    def table_from_columns(
        self,
        columns: Sequence[sa.Column],
        *,
        schema: Optional[str] = None,
        table_name: Optional[str] = None,
    ) -> sa.Table:
        table_name = table_name or f"test_table_{shortuuid.uuid()[:10]}"
        table = sa.Table(table_name, sa.MetaData(), *columns, schema=schema)
        return table

    def create_table(self, table: sa.Table) -> None:
        table_description = self._generate_table_description(table.columns)
        table_path = self._get_table_path(table)
        connection_params = self._get_connection_params()
        driver = ydb.Driver(connection_params)
        driver.wait(timeout=5)
        session = driver.table_client.session().create()
        session.create_table(table_path, table_description)
        driver.stop(timeout=5)

    def insert_into_table(self, table: sa.Table, data: Sequence[dict]) -> None:
        connection_params = ydb.DriverConfig(
            endpoint=self.engine.url.query["endpoint"],
            database=self.engine.url.query["database"],
        )
        driver = ydb.Driver(connection_params)
        driver.wait(timeout=5)
        session = driver.table_client.session().create()

        table_path = self._get_table_path(table)

        upsert_query_prefix = f"""
        $date_parse = DateTime::Parse("%Y-%m-%d");
        $datetime_parse = DateTime::Parse("%Y-%m-%d %H:%M:%S");
        UPSERT INTO `{table_path}` ({", ".join([column.name for column in table.columns])}) VALUES
        """
        upserts = (
            "({})".format(
                ", ".join(
                    [
                        (
                            "NULL"
                            if data[column.name] is None
                            else SA_TYPE_TO_YDB_TYPE[type(column.type)].to_sql_str(data[column.name])
                        )
                        for column in table.columns
                    ]
                )
            )
            for data in data
        )
        session.transaction().execute(upsert_query_prefix + ",\n".join(upserts) + ";", commit_tx=True)
        driver.stop(timeout=5)

    def drop_table(self, db_name: str, table: sa.Table) -> None:
        connection_params = self._get_connection_params()
        driver = ydb.Driver(connection_params)
        driver.wait(timeout=5)
        session = driver.table_client.session().create()
        table_path = self._get_table_path(table)

        try:
            session.drop_table(table_path)
        except ydb.issues.SchemeError as err:
            if "does not exist" in str(err):
                pass  # Table does not exist
            else:
                raise

        driver.stop(timeout=5)
