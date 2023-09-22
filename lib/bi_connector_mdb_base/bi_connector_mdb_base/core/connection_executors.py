from typing import Protocol

import attr

from bi_api_lib_ya.connections_security.base import MDBDomainManager
from dl_core.connection_executors.models.connection_target_dto_base import BaseSQLConnTargetDTO
from dl_core.connections_security.base import ConnectionSecurityManager

from bi_connector_mdb_base.core.us_connection import _MDBConnectOptionsProtocol


class _MDBConnectionProtocol(Protocol):
    _sec_mgr: ConnectionSecurityManager
    _conn_options: _MDBConnectOptionsProtocol

    async def _make_target_conn_dto_pool(self) -> list[BaseSQLConnTargetDTO]:
        ...


@attr.s(cmp=False, hash=False)
class MDBHostConnExecutorMixin:
    async def _make_target_conn_dto_pool(self: _MDBConnectionProtocol) -> list[BaseSQLConnTargetDTO]:
        super_dto_pool = await super()._make_target_conn_dto_pool()  # type: ignore
        dto_pool: list[BaseSQLConnTargetDTO] = []
        for dto in super_dto_pool:
            if self._conn_options.use_managed_network:
                db_domain_manager = self._sec_mgr.db_domain_manager
                assert isinstance(db_domain_manager, MDBDomainManager)
                effective_host = await db_domain_manager.normalize_mdb_host(dto.host)
                dto_pool.append(dto.clone(host=effective_host))
            else:
                dto_pool.append(dto)
        return dto_pool
