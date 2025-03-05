import ssl
from typing import (
    Any,
    Optional,
)

import attr
import requests
from requests.adapters import HTTPAdapter
from trino.auth import (
    BasicAuthentication,
    JWTAuthentication,
)
from trino.sqlalchemy import URL as trino_url

from dl_core.connection_executors.adapters.adapters_base_sa_classic import BaseClassicAdapter
from dl_core.connection_executors.models.db_adapter_data import DBAdapterQuery
from dl_core.connection_models.common_models import (
    DBIdent,
    SchemaIdent,
    TableIdent,
)

from dl_connector_trino.core.constants import (
    ADAPTER_SOURCE_NAME,
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

    def get_conn_line(self, db_name: Optional[str] = None, params: Optional[dict[str, Any]] = None) -> str:
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
            "http_scheme": "http" if self._target_dto.auth_type is TrinoAuthType.NONE else "https",
        }
        if self._target_dto.auth_type is TrinoAuthType.NONE:
            pass
        elif self._target_dto.auth_type is TrinoAuthType.PASSWORD:
            args["auth"] = BasicAuthentication(self._target_dto.username, self._target_dto.password)
        elif self._target_dto.auth_type is TrinoAuthType.JWT:
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
        result = self.execute(DBAdapterQuery("SELECT VERSION()"))
        return result.get_all()[0][0]

    def _get_catalogs(self) -> list[str]:
        result = self.execute(DBAdapterQuery("SHOW CATALOGS"))
        return [row[0] for row in result.get_all() if row[0] not in TRINO_SYSTEM_CATALOGS]

    def _get_schema_names(self, db_ident: DBIdent) -> list[str]:
        # Trino doesn't require db_name to connect, but has catalogs. Idea is to incorporate catalogs into schema_names.
        # At some point it may explode, but we'll exactly know where our assumptions are.
        assert db_ident.db_name is None

        schema_names: list[str] = []  # schema_names in "catalog.schema" format.
        catalogs = self._get_catalogs()
        for catalog in catalogs:
            result = self.execute(DBAdapterQuery(f"SHOW SCHEMAS FROM {catalog}"))
            for row in result.get_all():
                if row[0] in TRINO_SYSTEM_SCHEMAS:
                    continue

                schema_names.append(f"{catalog}.{row[0]}")

        return schema_names

    def _get_tables(self, schema_ident: SchemaIdent) -> list[TableIdent]:
        if schema_ident.schema_name is not None:
            source_name = str(schema_ident)
        else:
            assert schema_ident.db_name is not None
            source_name = schema_ident.db_name

        query = f"SHOW TABLES FROM {source_name}"
        result = self.execute(DBAdapterQuery(query))
        return [
            TableIdent(
                db_name=schema_ident.db_name,
                schema_name=schema_ident.schema_name,
                table_name=row[0],
            )
            for row in result.get_all()
        ]
