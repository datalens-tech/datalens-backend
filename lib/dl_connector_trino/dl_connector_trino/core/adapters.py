import ssl
from typing import (
    Any,
    Callable,
    Optional,
)

import attr
import requests
from requests.adapters import HTTPAdapter
import sqlalchemy as sa
from sqlalchemy.engine import Engine
from trino.auth import BasicAuthentication
from trino.dbapi import connect as trino_connect

from dl_core.connection_executors.adapters.adapters_base_sa import BaseSAAdapter
from dl_core.connection_executors.models.db_adapter_data import DBAdapterQuery
from dl_core.connection_models.common_models import (
    DBIdent,
    SchemaIdent,
    TableIdent,
)

from dl_connector_trino.core.constants import (
    CONNECTION_TYPE_TRINO,
    TrinoAuthType,
)
from dl_connector_trino.core.error_transformer import trino_error_transformer
from dl_connector_trino.core.target_dto import TrinoConnTargetDTO


TRINO_SYSTEM_CATALOGS = (
    "system",
    "tpch",
    "tpcds",
    "jmx",
)

TRINO_SYSTEM_SCHEMAS = ("information_schema",)


def construct_creator_func(target_dto: TrinoConnTargetDTO) -> Callable[[], sa.engine.Connection]:
    def get_connection() -> sa.engine.Connection:
        params = dict(
            host=target_dto.host,
            port=target_dto.port,
            user=target_dto.username,
            http_scheme="http" if target_dto.auth_type is TrinoAuthType.NONE else "https",
        )
        if target_dto.auth_type is TrinoAuthType.PASSWORD:
            params["auth"] = BasicAuthentication(target_dto.username, target_dto.password)
        elif target_dto.auth_type is TrinoAuthType.OAUTH2:
            raise NotImplementedError("OAuth2 authentication is not supported yet")
        elif target_dto.auth_type is TrinoAuthType.KERBEROS:
            raise NotImplementedError("Kerberos authentication is not supported yet")
        elif target_dto.auth_type is TrinoAuthType.CERTIFICATE:
            raise NotImplementedError("Certificate authentication is not supported yet")
        elif target_dto.auth_type is TrinoAuthType.JWT:
            raise NotImplementedError("JWT authentication is not supported yet")
        elif target_dto.auth_type is TrinoAuthType.HEADER:
            raise NotImplementedError("Header authentication is not supported yet")

        if target_dto.ssl_ca:

            class CustomHTTPAdapter(HTTPAdapter):
                def init_poolmanager(
                    self, connections: int, maxsize: int, block: bool = False, **pool_kwargs: Any
                ) -> None:
                    # Use a secure context with the provided SSL CA
                    context = ssl.create_default_context(cadata=target_dto.ssl_ca)
                    context.check_hostname = False  # TODO: @khamitovdr Resolve "ValueError: check_hostname requires server_hostname" and enable check_hostname!!!
                    super().init_poolmanager(connections, maxsize, block, ssl_context=context, **pool_kwargs)

            session = requests.Session()
            session.mount("https://", CustomHTTPAdapter())
            params["http_session"] = session

        conn = trino_connect(**params)
        return conn

    return get_connection


@attr.s(kw_only=True)
class TrinoDefaultAdapter(BaseSAAdapter[TrinoConnTargetDTO]):
    conn_type = CONNECTION_TYPE_TRINO
    _error_transformer = trino_error_transformer

    def get_default_db_name(self) -> Optional[str]:
        return None

    def get_db_name_for_query(self, db_name_from_query: Optional[str]) -> str:
        return ""  # Not my fault, it's unnecessary for Trino

    def _get_db_engine(self, db_name: str, disable_streaming: bool = False) -> Engine:
        if disable_streaming:
            raise Exception("`disable_streaming` is not applicable here")
        return sa.create_engine(
            "trino://",
            creator=construct_creator_func(self._target_dto),
        ).execution_options(compiled_cache=None)

    def _get_db_version(self, db_ident: DBIdent) -> str:
        result = self.execute(DBAdapterQuery("SELECT VERSION()"))
        return result.get_all()[0][0]

    def _get_catalogs(self) -> list[str]:
        result = self.execute(DBAdapterQuery("SHOW CATALOGS"))
        return [row[0] for row in result.get_all() if row[0] not in TRINO_SYSTEM_CATALOGS]

    def _get_schema_names(self, db_ident: DBIdent) -> list[str]:
        if db_ident.db_name is not None:  # No use cases for now
            result = self.execute(DBAdapterQuery(f"SHOW SCHEMAS FROM {db_ident.db_name}"))
            return [row[0] for row in result.get_all()]

        schema_names: list[str] = []  # schema_names in "catalog.schema" format.
        catalogs = self._get_catalogs()
        for catalog in catalogs:
            result = self.execute(DBAdapterQuery(f"SHOW SCHEMAS FROM {catalog}"))
            schema_names.extend(
                [f"{catalog}.{row[0]}" for row in result.get_all() if row[0] not in TRINO_SYSTEM_SCHEMAS]
            )

        return schema_names

    def _get_tables(self, schema_ident: SchemaIdent) -> list[TableIdent]:
        query = (
            f"SHOW TABLES FROM {schema_ident}"
            if schema_ident.schema_name is not None
            else f"SHOW TABLES FROM {schema_ident.db_name}"
        )
        result = self.execute(DBAdapterQuery(query))
        return [
            TableIdent(
                db_name=schema_ident.db_name,
                schema_name=schema_ident.schema_name,
                table_name=row[0],
            )
            for row in result.get_all()
        ]
