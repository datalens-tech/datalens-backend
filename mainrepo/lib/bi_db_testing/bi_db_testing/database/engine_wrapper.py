from __future__ import annotations

import contextlib
from typing import (
    Any,
    ClassVar,
    Generator,
    Optional,
    Sequence,
    Type,
)
import urllib.parse

import attr
from frozendict import frozendict
import shortuuid
import sqlalchemy as sa
from sqlalchemy.engine.base import (
    Connection,
    Engine,
)
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.engine.url import (
    URL,
    make_url,
)
import sqlalchemy.exc as sa_exc
from sqlalchemy.pool import (
    NullPool,
    Pool,
)

from bi_utils.wait import wait_for


@attr.s(frozen=True)
class DbEngineConfig:
    url: str | URL = attr.ib(kw_only=True)
    engine_params: frozendict[str, Any] = attr.ib(kw_only=True, converter=frozendict, factory=frozendict)


@attr.s(frozen=True)
class TableSpec:
    # db_name: str = attr.ib(kw_only=True)
    table_name: str = attr.ib(kw_only=True)


@attr.s
class EngineWrapperBase:
    URL_PREFIX: ClassVar[str]
    POOL_CLS: ClassVar[Type[Pool]] = NullPool
    TABLE_AVAILABILITY_TIMEOUT: ClassVar[float] = 0.0
    CONFIG_CLS: ClassVar[Type[DbEngineConfig]] = DbEngineConfig

    _config: DbEngineConfig = attr.ib(repr=False)

    # Internals
    _engine: Engine = attr.ib(init=False, default=None)
    _connection: Optional[Connection] = attr.ib(init=False, default=None, repr=False)

    def _db_connect_params(self) -> dict:
        return dict()

    def __attrs_post_init__(self) -> None:
        url = self._config.url
        connect_params = self._db_connect_params()
        if isinstance(url, str):
            if not url.startswith(self.URL_PREFIX):
                raise ValueError(
                    f"Unexpected URL prefix for {type(self)}: expected prefix {self.URL_PREFIX!r} in {url!r}"
                )
            if connect_params:
                url = f"{url}?{urllib.parse.urlencode(connect_params)}"
        else:
            assert isinstance(url, URL)
            if not url.drivername.startswith(self.URL_PREFIX):
                raise ValueError(f"Unexpected URL prefix for {type(self)}: {url}")
            if connect_params:
                url.query.update(connect_params)

        self._engine = sa.create_engine(
            url,
            poolclass=self.POOL_CLS,
            **self._config.engine_params,
        ).execution_options(compiled_cache=None)

    @property
    def config(self) -> DbEngineConfig:
        return self._config

    @property
    def engine(self) -> Engine:
        return self._engine

    @property
    def url(self) -> URL:
        return self._engine.url

    @property
    def dialect(self) -> DefaultDialect:
        return self._engine.dialect

    @property
    def connection(self) -> Connection:
        if self._connection is None:
            self._connection = self._engine.connect()

        assert self._connection is not None
        return self._connection

    def execute(self, query: Any, *multiparams: Any, **params: Any) -> sa.engine.CursorResult:
        return self._engine.execute(query, *multiparams, **params)

    def has_table(self, table_name: str, schema: Optional[str] = None) -> bool:
        return self.dialect.has_table(self._engine, table_name, schema=schema)

    def load_table(self, table_name: str, schema: Optional[str] = None) -> sa.Table:
        return sa.Table(table_name, sa.MetaData(bind=self.engine), schema=schema, autoload=True)

    def create_table(self, table: sa.Table) -> None:
        table.create(bind=self.engine)
        self.wait_for_table(table)

    def table_available(self, table: sa.Table) -> bool:
        """
        Check that the table is available for further actions.
        This might require checks different from `has_table`.
        """
        return self.has_table(table_name=table.name, schema=table.schema)

    def create_schema(self, schema_name: str) -> None:
        self.execute(f"CREATE SCHEMA {self.quote(schema_name)}")

    def wait_for_table(self, table: sa.Table) -> None:
        if self.TABLE_AVAILABILITY_TIMEOUT:
            wait_for(
                name=f"Table {table.name}",
                condition=lambda: self.table_available(table),
                timeout=self.TABLE_AVAILABILITY_TIMEOUT,
            )

    def drop_table(self, db_name: str, table: sa.Table) -> None:
        table.drop(bind=self.engine, checkfirst=True)

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

    def count_sql_sessions(self) -> int:
        return 0

    def quote(self, value: str) -> str:
        return self.dialect.identifier_preparer.quote(value)

    def get_conn_credentials(self, full: bool = False) -> dict:
        return dict(
            host=self.url.host,
            port=int(self.url.port),
            username=self.url.username,
            password=self.url.password,
            db_name=self.url.database,
        )

    def get_conn_hosts(self) -> tuple[str, ...]:
        host = self.get_conn_credentials(full=True).get("host")
        if host:
            return tuple(h.strip() for h in host.split(","))
        else:
            return ()

    @contextlib.contextmanager
    def connect(self) -> Generator[Connection, None, None]:
        connection = self._engine.connect(close_with_result=True)
        yield connection
        # Don't close immediately because the cursor may still be in use

    def insert_into_table(self, table: sa.Table, data: Sequence[dict]) -> None:
        self.execute(table.insert(data))

    def get_version(self) -> Optional[str]:
        return self.execute(sa.select([sa.func.version()])).scalar()

    def test(self) -> bool:
        try:
            self.execute(sa.select(1))
        except sa_exc.DatabaseError:
            return False
        else:
            return True


_REGISTERED_ENGINE_WRAPPERS: dict[str, Type[EngineWrapperBase]] = {}


def register_engine_wrapper_cls(engine_wrapper_cls: Type[EngineWrapperBase]) -> None:
    _REGISTERED_ENGINE_WRAPPERS[engine_wrapper_cls.URL_PREFIX] = engine_wrapper_cls


def get_engine_wrapper_cls_for_url(url: str | URL) -> Type[EngineWrapperBase]:
    if isinstance(url, str):
        url = make_url(url)
    assert isinstance(url, URL)
    schema = url.get_backend_name()
    return _REGISTERED_ENGINE_WRAPPERS[schema]
