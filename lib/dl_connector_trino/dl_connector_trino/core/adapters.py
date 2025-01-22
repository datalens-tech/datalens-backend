from __future__ import annotations

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

# from trino import sqlalchemy as tsa
from trino.dbapi import connect as trino_connect

from dl_core.connection_executors.adapters.adapters_base_sa import BaseSAAdapter
from dl_core.connection_executors.adapters.adapters_base_sa_classic import BaseClassicAdapter

from dl_connector_trino.core.constants import (
    CONNECTION_TYPE_TRINO,
    TrinoAuthType,
)
from dl_connector_trino.core.error_transformer import trino_error_transformer
from dl_connector_trino.core.target_dto import TrinoConnTargetDTO


# from dl_core.connection_executors.models.db_adapter_data import (
#     DBAdapterQuery,
#     RawSchemaInfo,
# )
# from dl_core.connection_models.common_models import (
#     DBIdent,
#     SATextTableDefinition,
# )
# from dl_type_transformer.native_type import SATypeSpec


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
        # As new auth types are supported, add them here

        if target_dto.ssl_ca:

            class CustomHTTPAdapter(HTTPAdapter):
                def init_poolmanager(
                    self, connections: int, maxsize: int, block: bool = False, **pool_kwargs: Any
                ) -> None:
                    # Use a secure context with the provided SSL CA
                    context = ssl.create_default_context(cadata=target_dto.ssl_ca)
                    super().init_poolmanager(connections, maxsize, block, ssl_context=context, **pool_kwargs)

            session = requests.Session()
            session.mount("https://", CustomHTTPAdapter())
            params["session"] = session

        conn = trino_connect(**params)
        return conn

    return get_connection


@attr.s(kw_only=True)
class TrinoDefaultAdapter(BaseClassicAdapter, BaseSAAdapter[TrinoConnTargetDTO]):
    conn_type = CONNECTION_TYPE_TRINO
    _error_transformer = trino_error_transformer

    def get_default_db_name(self) -> Optional[str]:
        return None

    def _get_db_engine(self, db_name: str, disable_streaming: bool = False) -> Engine:
        if disable_streaming:
            raise Exception("`disable_streaming` is not applicable here")
        return sa.create_engine(
            "trino://",
            creator=construct_creator_func(self._target_dto),
        ).execution_options(compiled_cache=None)
