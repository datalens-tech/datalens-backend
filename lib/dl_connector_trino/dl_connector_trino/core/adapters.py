from collections.abc import Generator
import datetime
import ssl
from typing import Any

import attr
import requests
from requests.adapters import HTTPAdapter
import sqlalchemy as sa
from sqlalchemy import exc as sa_exc
from sqlalchemy import types as sqltypes
from trino.auth import (
    BasicAuthentication,
    JWTAuthentication,
)
from trino.sqlalchemy import URL as trino_url
from trino.sqlalchemy.compiler import TrinoSQLCompiler
from trino.sqlalchemy.datatype import parse_sqltype
from trino.sqlalchemy.dialect import TrinoDialect

import dl_configs
from dl_core.connection_executors.adapters.adapters_base_sa_classic import BaseClassicAdapter
from dl_core.connection_executors.models.db_adapter_data import (
    DBAdapterQuery,
    ExecutionStep,
)
from dl_core.connection_models.common_models import (
    DBIdent,
    PageIdent,
    SchemaIdent,
    TableIdent,
)
from dl_type_transformer.native_type import SATypeSpec

from dl_connector_trino.core.constants import (
    ADAPTER_SOURCE_NAME,
    CONNECTION_TYPE_TRINO,
    TrinoAuthType,
)
from dl_connector_trino.core.error_transformer import (
    ExpressionNotAggregateError,
    trino_error_transformer,
)
from dl_connector_trino.core.target_dto import TrinoConnTargetDTO


TRINO_SYSTEM_SCHEMAS = ("information_schema",)


TRINO_TABLES = sa.Table(
    "tables",
    sa.MetaData(),
    sa.Column("table_schema", sa.String),
    sa.Column("table_name", sa.String),
    schema="information_schema",
)
GET_TRINO_TABLES_QUERY = (
    sa.select(
        TRINO_TABLES.c.table_schema,
        TRINO_TABLES.c.table_name,
    )
    .where(
        ~TRINO_TABLES.c.table_schema.in_(TRINO_SYSTEM_SCHEMAS),
    )
    .order_by(
        TRINO_TABLES.c.table_schema,
        TRINO_TABLES.c.table_name,
    )
)


class CustomHTTPAdapter(HTTPAdapter):
    """
    This custom adapter is here to create an SSL context with a custom CA certificate provided as a string instead of a file path.
    """

    def __init__(self, ssl_ca: str | None = None, *args: Any, **kwargs: Any) -> None:
        self.ssl_ca = ssl_ca
        super().__init__(*args, **kwargs)

    def init_poolmanager(self, connections: int, maxsize: int, block: bool = False, **pool_kwargs: Any) -> None:
        if self.ssl_ca is None:
            context = dl_configs.get_default_ssl_context()
        else:
            context = ssl.create_default_context(cadata=self.ssl_ca)
        super().init_poolmanager(connections, maxsize, block, ssl_context=context, **pool_kwargs)


class CustomTrinoCompiler(TrinoSQLCompiler):
    def render_literal_value(self, value: Any, type_: sqltypes.TypeEngine) -> str:
        if isinstance(type_, sqltypes.Date) and isinstance(value, datetime.date):
            return f"DATE '{value.strftime('%Y-%m-%d')}'"

        if isinstance(type_, sqltypes.DateTime) and isinstance(value, datetime.datetime):
            datetime_repr = value.strftime("%Y-%m-%d %H:%M:%S")

            if value.microsecond:
                datetime_repr += f".{value.microsecond:06}"

            if value.tzinfo is None:
                return f"TIMESTAMP '{datetime_repr}'"
            elif value.tzinfo == datetime.timezone.utc:
                timezone_repr = "UTC"
            elif hasattr(value.tzinfo, "zone"):
                # This is a pytz timezone object
                timezone_repr = value.tzinfo.zone
            else:
                raise TypeError(f"Unsupported tzinfo type: {type(value.tzinfo)}")

            return f"TIMESTAMP '{datetime_repr} {timezone_repr}'"

        if isinstance(type_, sqltypes.ARRAY) and isinstance(value, (list, tuple)):
            array_elements = ", ".join(self.render_literal_value(v, type_.item_type) for v in value)
            return f"ARRAY[{array_elements}]"

        return super().render_literal_value(value, type_)


class CustomTrinoDialect(TrinoDialect):
    statement_compiler = CustomTrinoCompiler


@attr.s(kw_only=True)
class TrinoDefaultAdapter(BaseClassicAdapter[TrinoConnTargetDTO]):
    conn_type = CONNECTION_TYPE_TRINO
    _error_transformer = trino_error_transformer
    _db_version: str | None = None

    EXTRA_EXC_CLS = (sa_exc.DBAPIError,)

    def get_conn_line(self, db_name: str | None = None, params: dict[str, Any] | None = None) -> str:
        params = params or {}
        return trino_url(
            host=self._target_dto.host,
            port=self._target_dto.port,
            user=self._target_dto.username,
            catalog=db_name,
            source=ADAPTER_SOURCE_NAME,
            **params,
        )

    def _get_http_session(self) -> requests.Session:
        session = requests.Session()
        adapter = CustomHTTPAdapter(ssl_ca=self._target_dto.ssl_ca)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def get_connect_args(self) -> dict[str, Any]:
        timeout = (
            self._target_dto.connect_timeout,
            None,  # read timeout is handled by trino with query_max_run_time
        )
        args: dict[str, Any] = super().get_connect_args() | dict(
            http_scheme="https" if self._target_dto.ssl_enable else "http",
            http_session=self._get_http_session(),
            session_properties=dict(
                query_max_run_time=f"{self._target_dto.total_timeout}s",
            ),
            legacy_primitive_types=True,
            request_timeout=timeout,
        )
        if self._target_dto.auth_type is TrinoAuthType.none:
            pass
        elif self._target_dto.auth_type is TrinoAuthType.password:
            args["auth"] = BasicAuthentication(self._target_dto.username, self._target_dto.password)
        elif self._target_dto.auth_type is TrinoAuthType.jwt:
            args["auth"] = JWTAuthentication(self._target_dto.jwt)
        else:
            raise NotImplementedError(f"{self._target_dto.auth_type.name} authentication is not supported yet")

        return args

    def execute_by_steps(self, db_adapter_query: DBAdapterQuery) -> Generator[ExecutionStep, None, None]:
        try:
            for result in super().execute_by_steps(db_adapter_query):
                yield result
        except ExpressionNotAggregateError:
            query = db_adapter_query.query
            compiled_query = (
                query
                if isinstance(query, str)
                else str(query.compile(dialect=CustomTrinoDialect(), compile_kwargs={"literal_binds": True}))
            )
            db_adapter_compiled_query = db_adapter_query.clone(query=compiled_query)

            for result in super().execute_by_steps(db_adapter_compiled_query):
                yield result

    def get_default_db_name(self) -> str:
        return ""  # Trino doesn't require db_name to connect.

    def _get_db_version(self, db_ident: DBIdent) -> str:
        if self._db_version is None:
            result = self.execute(DBAdapterQuery(sa.text("SELECT version()"))).get_all()
            self._db_version = result[0][0]

        return self._db_version

    def _get_tables(self, schema_ident: SchemaIdent, page_ident: PageIdent | None = None) -> list[TableIdent]:
        """
        Regardless accepting schema_ident, this method returns all tables from the catalog (schema_ident.db_name).
        schema_ident.schema_name is ignored.
        """
        result = self.execute(DBAdapterQuery(GET_TRINO_TABLES_QUERY, db_name=schema_ident.db_name))
        return [
            TableIdent(
                db_name=schema_ident.db_name,
                schema_name=schema_name,
                table_name=table_name,
            )
            for schema_name, table_name in result.get_all()
        ]

    def _cursor_column_to_sa(self, cursor_col: tuple[Any, ...], require: bool = True) -> SATypeSpec | None:
        return parse_sqltype(cursor_col[1])
