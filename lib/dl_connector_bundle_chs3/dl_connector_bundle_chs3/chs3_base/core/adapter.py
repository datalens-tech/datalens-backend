from functools import reduce
import logging

from aiohttp import ClientResponse
import attr

from dl_core.connection_executors.adapters.adapter_actions.async_base import AsyncDBVersionAdapterAction
from dl_core.connection_executors.adapters.adapter_actions.db_version import AsyncDBVersionAdapterActionEmptyString
from dl_core.connection_executors.models.db_adapter_data import DBAdapterQuery
from dl_core.connection_models import TableIdent

from dl_connector_bundle_chs3.chs3_base.core.target_dto import BaseFileS3ConnTargetDTO
from dl_connector_clickhouse.core.clickhouse_base.adapters import BaseAsyncClickHouseAdapter
from dl_connector_clickhouse.core.clickhouse_base.ch_commons import (
    ClickHouseBaseUtils,
    get_ch_settings,
)


class FileS3Utils(ClickHouseBaseUtils):
    pass


LOGGER = logging.getLogger(__name__)


@attr.s(kw_only=True)
class BaseAsyncFileS3Adapter(BaseAsyncClickHouseAdapter):
    ch_utils = FileS3Utils
    _target_dto: BaseFileS3ConnTargetDTO = attr.ib()  # type: ignore  # TODO: FIX

    def _make_async_db_version_action(self) -> AsyncDBVersionAdapterAction:
        return AsyncDBVersionAdapterActionEmptyString()

    async def is_table_exists(self, table_ident: TableIdent) -> bool:
        return True

    def get_request_params(self, dba_q: DBAdapterQuery) -> dict[str, str]:  # test
        return dict(
            # TODO FIX: Move to utils
            database=dba_q.db_name or self._target_dto.db_name or "system",
            **get_ch_settings(
                read_only_level=2,
            ),
        )

    async def _make_query(self, dba_q: DBAdapterQuery, mirroring_mode: bool = False) -> ClientResponse:
        if not isinstance(dba_q.query, str):
            dialect = self.get_dialect()
            query_str_raw = dba_q.query.compile(dialect=dialect, compile_kwargs={"literal_binds": True}).string
        else:
            query_str_raw = dba_q.query

        if query_str_raw.startswith("select version()"):  # skip formatting for special queries
            return await super()._make_query(dba_q, mirroring_mode)

        replace_secret = self._target_dto.replace_secret
        secrets = (
            (f"key_id_{replace_secret}", self._target_dto.access_key_id),
            (f"secret_key_{replace_secret}", self._target_dto.secret_access_key),
        )
        secrets_hidden = (
            (f"key_id_{replace_secret}", "<hidden>"),
            (f"secret_key_{replace_secret}", "<hidden>"),
        )

        debug_query = dba_q.debug_compiled_query if dba_q.debug_compiled_query is not None else query_str_raw

        dba_q = dba_q.clone(
            query=reduce(lambda a, kv: a.replace(*kv), secrets, query_str_raw) % (),
            debug_compiled_query=reduce(lambda a, kv: a.replace(*kv), secrets_hidden, debug_query) % (),
        )
        return await super()._make_query(dba_q, mirroring_mode)
