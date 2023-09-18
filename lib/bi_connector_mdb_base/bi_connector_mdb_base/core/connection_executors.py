from typing import Protocol

import attr

from dl_core.connection_executors.models.connection_target_dto_base import BaseSQLConnTargetDTO
from dl_core.mdb_utils import MDBDomainManager


class _MDBConnectionProtocol(Protocol):
    _mdb_mgr: MDBDomainManager

    async def _make_target_conn_dto_pool(self) -> list[BaseSQLConnTargetDTO]:
        ...


@attr.s(cmp=False, hash=False)
class MDBHostConnExecutorMixin:
    _mdb_mgr: "MDBDomainManager" = attr.ib()

    async def _make_target_conn_dto_pool(self: _MDBConnectionProtocol) -> list[BaseSQLConnTargetDTO]:
        super_dto_pool = await super()._make_target_conn_dto_pool()  # type: ignore
        dto_pool: list[BaseSQLConnTargetDTO] = []
        for dto in super_dto_pool:
            effective_host = await self._mdb_mgr.normalize_mdb_host(dto.host)
            dto_pool.append(dto.clone(host=effective_host))
        return dto_pool
