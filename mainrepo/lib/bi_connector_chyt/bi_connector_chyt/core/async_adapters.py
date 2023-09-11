from __future__ import annotations

import logging

import attr

from clickhouse_sqlalchemy.drivers.http.transport import _get_type  # noqa

from bi_core.connectors.clickhouse_base.adapters import BaseAsyncClickHouseAdapter
from bi_core.utils import get_current_w3c_tracing_headers

from bi_connector_chyt.core.constants import CONNECTION_TYPE_CHYT
from bi_connector_chyt.core.target_dto import BaseCHYTConnTargetDTO
from bi_connector_chyt.core.utils import CHYTUtils


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
