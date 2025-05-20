import ssl
from typing import Any

import attr
import requests
from requests.adapters import HTTPAdapter
import sqlalchemy as sa
from trino.auth import (
    BasicAuthentication,
    JWTAuthentication,
)
from trino.sqlalchemy import URL as trino_url
from trino.sqlalchemy.datatype import parse_sqltype

from dl_core.connection_executors.adapters.adapters_base_sa_classic import BaseClassicAdapter
from dl_core.connection_executors.models.db_adapter_data import DBAdapterQuery
from dl_core.connection_models.common_models import (
    DBIdent,
    SchemaIdent,
    TableIdent,
)
from dl_type_transformer.native_type import SATypeSpec

from dl_connector_trino.core.constants import (
    ADAPTER_SOURCE_NAME,
    CONNECTION_TYPE_TRINO,
    TrinoAuthType,
)
from dl_connector_trino.core.error_transformer import trino_error_transformer
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
    def __init__(self, ssl_ca: str, *args: Any, **kwargs: Any) -> None:
        self.ssl_ca = ssl_ca
        super().__init__(*args, **kwargs)

    def init_poolmanager(self, connections: int, maxsize: int, block: bool = False, **pool_kwargs: Any) -> None:
        # Use a secure context with the provided SSL CA
        context = ssl.create_default_context(cadata=self.ssl_ca)
        context.check_hostname = False  # TODO: @khamitovdr Resolve "ValueError: check_hostname requires server_hostname" and enable check_hostname!!!
        super().init_poolmanager(connections, maxsize, block, ssl_context=context, **pool_kwargs)


@attr.s(kw_only=True)
class TrinoDefaultAdapter(BaseClassicAdapter[TrinoConnTargetDTO]):
    conn_type = CONNECTION_TYPE_TRINO
    _error_transformer = trino_error_transformer
    _db_version: str | None = None

    def get_conn_line(self, db_name: str | None = None, params: dict[str, Any] | None = None) -> str:
        # We do not expect to transfer any additional parameters when creating the engine.
        # This check is needed to track if it still passed.
        assert params is None

        params = params or {}
        return trino_url(
            host=self._target_dto.host,
            port=self._target_dto.port,
            user=self._target_dto.username,
            catalog=db_name,
            source=ADAPTER_SOURCE_NAME,
            **params,
        )

    def get_connect_args(self) -> dict[str, Any]:
        args: dict[str, Any] = {
            **super().get_connect_args(),
            "legacy_primitive_types": True,
            "http_scheme": "http" if self._target_dto.auth_type is TrinoAuthType.none else "https",
        }
        if self._target_dto.auth_type is TrinoAuthType.none:
            pass
        elif self._target_dto.auth_type is TrinoAuthType.password:
            args["auth"] = BasicAuthentication(self._target_dto.username, self._target_dto.password)
        elif self._target_dto.auth_type is TrinoAuthType.jwt:
            args["auth"] = JWTAuthentication(self._target_dto.jwt)
        else:
            raise NotImplementedError(f"{self._target_dto.auth_type.name} authentication is not supported yet")

        if self._target_dto.ssl_ca:
            session = requests.Session()
            session.mount("https://", CustomHTTPAdapter(self._target_dto.ssl_ca))
            args["http_session"] = session

        return args

    def get_default_db_name(self) -> str:
        return ""  # Trino doesn't require db_name to connect.

    def _get_db_version(self, db_ident: DBIdent) -> str:
        if self._db_version is None:
            result = self.execute(DBAdapterQuery(sa.text("SELECT version()"))).get_all()
            self._db_version = result[0][0]

        return self._db_version

    def _get_tables(self, schema_ident: SchemaIdent) -> list[TableIdent]:
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
