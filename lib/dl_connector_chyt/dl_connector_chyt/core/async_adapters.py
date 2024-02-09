from __future__ import annotations

import logging

from aiohttp import ClientResponse
import attr
from clickhouse_sqlalchemy.drivers.http.transport import _get_type  # noqa

from dl_core.connection_executors.models.db_adapter_data import DBAdapterQuery
from dl_core.utils import get_current_w3c_tracing_headers

from dl_connector_chyt.core.constants import CONNECTION_TYPE_CHYT
from dl_connector_chyt.core.target_dto import BaseCHYTConnTargetDTO
from dl_connector_chyt.core.utils import CHYTUtils
from dl_connector_clickhouse.core.clickhouse_base.adapters import BaseAsyncClickHouseAdapter


LOGGER = logging.getLogger(__name__)


@attr.s(kw_only=True)
class AsyncCHYTAdapter(BaseAsyncClickHouseAdapter):
    ch_utils = CHYTUtils
    conn_type = CONNECTION_TYPE_CHYT

    _target_dto: BaseCHYTConnTargetDTO = attr.ib()

    def _get_current_tracing_headers(self) -> dict[str, str]:
        return get_current_w3c_tracing_headers(
            override_sampled=self.ch_utils.get_tracing_sample_flag_override(self._req_ctx_info),
            req_id=self._req_ctx_info.request_id,
        )

    async def _make_query(self, dba_q: DBAdapterQuery, mirroring_mode: bool = False) -> ClientResponse:
        resp = await super()._make_query(dba_q=dba_q, mirroring_mode=mirroring_mode)
        LOGGER.info(f"CHYT Response headers: {resp.headers}")
        return resp
