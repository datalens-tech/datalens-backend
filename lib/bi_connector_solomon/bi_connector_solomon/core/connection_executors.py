from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

import attr

from bi_api_commons_ya_team.constants import DLCookiesYT

from dl_core.connection_executors.async_sa_executors import DefaultSqlAlchemyConnExecutor
from bi_connector_solomon.core.adapter import AsyncSolomonAdapter
from bi_connector_solomon.core.target_dto import SolomonConnTargetDTO

if TYPE_CHECKING:
    from bi_connector_solomon.core.dto import SolomonConnDTO


@attr.s(cmp=False, hash=False)
class SolomonAsyncAdapterConnExecutor(DefaultSqlAlchemyConnExecutor[AsyncSolomonAdapter]):
    TARGET_ADAPTER_CLS = AsyncSolomonAdapter

    _conn_dto: SolomonConnDTO = attr.ib()

    async def _make_target_conn_dto_pool(self) -> Sequence[SolomonConnTargetDTO]:
        assert self._req_ctx_info is not None
        _ctx_cookies = dict()
        if self._req_ctx_info.auth_data is not None:
            _ctx_cookies = self._req_ctx_info.auth_data.get_cookies()
        return [SolomonConnTargetDTO(
            conn_id=self._conn_dto.conn_id,
            pass_db_messages_to_user=self._conn_options.pass_db_messages_to_user,
            pass_db_query_to_user=self._conn_options.pass_db_query_to_user,
            host=self._conn_dto.host,
            cookie_session_id=_ctx_cookies.get(DLCookiesYT.YA_TEAM_SESSION_ID),
            cookie_sessionid2=_ctx_cookies.get(DLCookiesYT.YA_TEAM_SESSION_ID_2),
            user_ip=self._req_ctx_info.client_ip,
            user_host=self._req_ctx_info.host,
        )]
