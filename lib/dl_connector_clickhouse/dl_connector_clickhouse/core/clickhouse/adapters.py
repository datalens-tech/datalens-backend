import logging

import attr

from dl_core.connection_executors.models.db_adapter_data import DBAdapterQuery

from dl_connector_clickhouse.core.clickhouse.target_dto import DLClickHouseConnTargetDTO
from dl_connector_clickhouse.core.clickhouse_base.adapters import (
    AsyncClickHouseAdapter,
    ClickHouseAdapter,
)
from dl_connector_clickhouse.core.clickhouse_base.ch_commons import ClickHouseUtils
from dl_connector_clickhouse.core.clickhouse_base.constants import CONNECTION_TYPE_CLICKHOUSE


LOGGER = logging.getLogger(__name__)


class DLClickHouseAdapter(ClickHouseAdapter):
    ...


class DLAsyncClickHouseAdapter(AsyncClickHouseAdapter):
    conn_type = CONNECTION_TYPE_CLICKHOUSE
    ch_utils = ClickHouseUtils

    _target_dto: DLClickHouseConnTargetDTO = attr.ib()

    def _get_readonly_param(self, dba_q: DBAdapterQuery) -> int | None:
        if dba_q.trusted_query:
            return None
        if self._target_dto.readonly == 1:
            return 1
        return 2
