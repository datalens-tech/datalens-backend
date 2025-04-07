from __future__ import annotations

import logging

import attr

from dl_core.connection_executors.models.db_adapter_data import DBAdapterQuery

from dl_connector_clickhouse.core.clickhouse.target_dto import DLClickHouseConnTargetDTO
from dl_connector_clickhouse.core.clickhouse_base.adapters import (
    AsyncClickHouseAdapter,
    ClickHouseAdapter,
)
from dl_connector_clickhouse.core.clickhouse_base.ch_commons import (
    ClickHouseUtils,
    get_ch_settings,
)
from dl_connector_clickhouse.core.clickhouse_base.constants import CONNECTION_TYPE_CLICKHOUSE


LOGGER = logging.getLogger(__name__)


class DLClickHouseAdapter(ClickHouseAdapter):
    ...


class DLAsyncClickHouseAdapter(AsyncClickHouseAdapter):
    conn_type = CONNECTION_TYPE_CLICKHOUSE
    ch_utils = ClickHouseUtils

    _target_dto: DLClickHouseConnTargetDTO = attr.ib()

    def get_request_params(self, dba_q: DBAdapterQuery) -> dict[str, str]:
        if dba_q.trusted_query:
            read_only_level = None
        elif self._target_dto.readonly == 1:
            read_only_level = 1
        else:
            read_only_level = 2
        return dict(
            database=dba_q.db_name or self._target_dto.db_name or "system",
            **get_ch_settings(
                read_only_level=read_only_level,
            ),
        )
