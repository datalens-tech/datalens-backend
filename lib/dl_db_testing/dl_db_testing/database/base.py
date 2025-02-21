from __future__ import annotations

import contextlib
from typing import (
    Any,
    Generator,
    Generic,
    List,
    Optional,
    Sequence,
    TypeVar,
    Union,
)

import attr
import sqlalchemy as sa
from sqlalchemy.engine import Connection
import sqlalchemy.engine.url
import sqlalchemy.exc as sa_exc

import dl_db_testing.database.engine_wrapper as ew


@attr.s(frozen=True)
class DbConfig:
    engine_config: ew.DbEngineConfig = attr.ib(kw_only=True)
    supports_executemany: bool = attr.ib(kw_only=True, default=True)


_DB_CONFIG_TV = TypeVar("_DB_CONFIG_TV", bound=DbConfig)


@attr.s
class DbBase(Generic[_DB_CONFIG_TV]):
    _engine_wrapper: ew.EngineWrapperBase = attr.ib(kw_only=True)
    _config: _DB_CONFIG_TV = attr.ib(kw_only=True)

    @property
    def engine(self) -> sqlalchemy.engine.Engine:
        return self._engine_wrapper.engine

    @property
    def engine_config(self) -> ew.DbEngineConfig:
        return self._engine_wrapper.config

    @property
    def config(self) -> _DB_CONFIG_TV:
        return self._config

    @property
    def url(self) -> sqlalchemy.engine.url.URL:
        return self._engine_wrapper.url

    def get_conn_credentials(self, full: bool = False) -> dict:
        return self._engine_wrapper.get_conn_credentials(full=full)

    @property
    def name(self) -> str:
        return self.get_conn_credentials(full=True)["db_name"]

    def count_sql_sessions(self) -> int:
        return self._engine_wrapper.count_sql_sessions()

    def get_conn_hosts(self) -> tuple[str, ...]:
        return self._engine_wrapper.get_conn_hosts()

    def quote(self, value: str) -> str:
        return self._engine_wrapper.quote(value)

    def expr_as_str(self, expr: sa.sql.ClauseElement) -> str:
        try:
            return expr.compile(
                compile_kwargs={"literal_binds": True},
                dialect=self._engine_wrapper.dialect,
            ).string
        except (NotImplementedError, sa_exc.CompileError):
            return str(expr)

    def execute(self, query: Any, *multiparams: Any, **params: Any) -> Any:
        return self._engine_wrapper.execute(query, *multiparams, **params)

    def scalar(self, expr: Any, from_: Optional[sa.sql.ClauseElement] = None) -> Any:
        assert isinstance(expr, sa.sql.ClauseElement)
        return self.execute(sa.select([expr]), from_=from_).scalar()

    def has_table(self, table_name: str) -> bool:
        return self._engine_wrapper.has_table(table_name)

    def get_current_connection(self) -> Connection:
        # FIXME: Remove
        return self._engine_wrapper.connection

    @contextlib.contextmanager
    def connect(self) -> Generator[Connection, None, None]:
        with self._engine_wrapper.connect() as connection:
            yield connection

    def drop_table(self, table: sa.Table) -> None:
        self._engine_wrapper.drop_table(db_name=self.name, table=table)

    def create_table(self, table: sa.Table) -> None:
        self.drop_table(table)
        self._engine_wrapper.create_table(table)

    def create_view(self, query: str, name: str, schema: Optional[str] = None) -> None:
        name = ".".join([self.quote(part) for part in (schema, name) if part])
        self.execute(f"CREATE VIEW {name} AS {query}")

    def load_table(self, table_name: str, schema: Optional[str] = None) -> sa.Table:
        return self._engine_wrapper.load_table(schema=schema, table_name=table_name)

    def table_from_columns(
        self,
        columns: Sequence[sa.Column],
        *,
        schema: Optional[str] = None,
        table_name: Optional[str] = None,
    ) -> sa.Table:
        return self._engine_wrapper.table_from_columns(
            columns=columns,
            schema=schema,
            table_name=table_name,
        )

    def insert_into_table(self, table: sa.Table, data: Sequence[dict]) -> None:
        self._engine_wrapper.insert_into_table(table=table, data=data)

    def create_schema(self, schema_name: str) -> None:
        self._engine_wrapper.create_schema(schema_name=schema_name)

    def base_eval(self, expr: sa.sql.ClauseElement, from_: Optional[sa.sql.ClauseElement] = None) -> Any:
        query = sa.select([expr])
        if from_ is not None:
            query = query.select_from(from_)

        try:
            value = self.execute(query).scalar()
            return value
        except Exception:
            print("QUERY:", self.expr_as_str(query))
            raise

    def get_version(self) -> Optional[str]:
        return self._engine_wrapper.get_version()

    def test(self) -> bool:
        return self._engine_wrapper.test()


@attr.s(frozen=True)
class DbTableBase:
    db: DbBase = attr.ib(kw_only=True)
    table: sa.Table = attr.ib(kw_only=True)
    schema: Optional[str] = attr.ib(kw_only=True, default=None)
    can_insert: bool = attr.ib(kw_only=True, default=True)

    @property
    def name(self) -> str:
        return self.table.name

    def insert(self, data: Union[dict, List[dict]], chunk_size: Optional[int] = None) -> None:
        chunk_size = chunk_size or 1000
        assert chunk_size is not None

        # TODO: Use insert_into_table
        if not self.can_insert:
            raise RuntimeError("Can't insert into table")
        if isinstance(data, dict):
            self.db.execute(self.table.insert(data))
        elif isinstance(data, list):
            # TODO: Change to itertools.batched after switching to Python 3.12
            for pos in range(0, len(data), chunk_size):
                chunk = data[pos : pos + chunk_size]
                if self.db.config.supports_executemany:
                    self.db.execute(self.table.insert(), chunk)
                else:
                    self.db.execute(self.table.insert(chunk))
        else:
            raise TypeError(type(data))

    @property
    def select_all_query(self) -> str:
        name = ".".join([self.db.quote(part) for part in (self.schema, self.name) if part])
        return f"SELECT * FROM {name}"
