from typing import List

import attr

from dl_core.connection_executors.async_sa_executors import DefaultSqlAlchemyConnExecutor

from bi_connector_gsheets.core.adapter import GSheetsDefaultAdapter
from bi_connector_gsheets.core.target_dto import GSheetsConnTargetDTO


@attr.s(cmp=False, hash=False)
class GSheetsAsyncAdapterConnExecutor(DefaultSqlAlchemyConnExecutor[GSheetsDefaultAdapter]):
    TARGET_ADAPTER_CLS = GSheetsDefaultAdapter

    async def _make_target_conn_dto_pool(self) -> List[GSheetsConnTargetDTO]:  # type: ignore  # TODO: fix
        return [
            GSheetsConnTargetDTO(
                conn_id=self._conn_dto.conn_id,
                pass_db_messages_to_user=self._conn_options.pass_db_messages_to_user,
                pass_db_query_to_user=self._conn_options.pass_db_query_to_user,
                sheets_url=self._conn_dto.sheets_url,  # type: ignore  # TODO: fix
                max_execution_time=self._conn_options.max_execution_time,  # type: ignore  # TODO: fix
                total_timeout=self._conn_options.total_timeout,  # type: ignore  # TODO: fix
                connect_timeout=self._conn_options.connect_timeout,  # type: ignore  # TODO: fix
                # use_gozora=self._conn_options.is_intranet,
                use_gozora=True,  # gsheets connector is left only in intranet
            )
        ]
