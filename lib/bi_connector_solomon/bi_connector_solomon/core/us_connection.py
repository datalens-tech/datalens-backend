from __future__ import annotations

from typing import Callable, ClassVar, Optional

import attr

from bi_constants.api_constants import DLCookiesCommon

from bi_core.base_models import ConnCacheableDataModelMixin
from bi_core.connection_executors.sync_base import SyncConnExecutorBase
from bi_core.us_connection_base import ConnectionBase, SubselectMixin, ExecutorBasedMixin, DataSourceTemplate
from bi_connector_solomon.core.dto import SolomonConnDTO


class SolomonConnection(SubselectMixin, ExecutorBasedMixin, ConnectionBase):
    allow_cache: ClassVar[bool] = True
    allow_dashsql: ClassVar[bool] = True

    @attr.s(kw_only=True)
    class DataModel(ConnCacheableDataModelMixin, ConnectionBase.DataModel):
        host: str = attr.ib()

    def get_conn_dto(self) -> SolomonConnDTO:
        _ctx_cookies = dict()
        if self._context.auth_data is not None:
            _ctx_cookies = self._context.auth_data.get_cookies()
        return SolomonConnDTO(
            conn_id=self.uuid,
            host=self.data.host,
            cookie_session_id=_ctx_cookies.get(DLCookiesCommon.YA_TEAM_SESSION_ID),
            cookie_sessionid2=_ctx_cookies.get(DLCookiesCommon.YA_TEAM_SESSION_ID_2),
            user_ip=self._context.client_ip,
            user_host=self._context.host,
        )

    @property
    def cache_ttl_sec_override(self) -> Optional[int]:
        return self.data.cache_ttl_sec

    @property
    def is_dashsql_allowed(self) -> bool:
        return True

    @property
    def is_subselect_allowed(self) -> bool:
        return False

    def get_data_source_templates(
            self, conn_executor_factory: Callable[[ConnectionBase], SyncConnExecutorBase],
    ) -> list[DataSourceTemplate]:
        return []

    @property
    def allow_public_usage(self) -> bool:
        return True
