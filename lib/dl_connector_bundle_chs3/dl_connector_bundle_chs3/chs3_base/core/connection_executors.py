import attr

from dl_connector_bundle_chs3.chs3_base.core.dto import BaseFileS3ConnDTO
from dl_connector_bundle_chs3.chs3_base.core.target_dto import BaseFileS3ConnTargetDTO
from dl_connector_clickhouse.core.clickhouse_base.conn_options import CHConnectOptions
from dl_connector_clickhouse.core.clickhouse_base.connection_executors import AsyncClickHouseConnExecutor


@attr.s(cmp=False, hash=False)
class BaseFileS3AsyncAdapterConnExecutor(AsyncClickHouseConnExecutor):
    _conn_dto: BaseFileS3ConnDTO = attr.ib()
    _conn_options: CHConnectOptions = attr.ib()

    async def _make_target_conn_dto_pool(self) -> list[BaseFileS3ConnTargetDTO]:  # type: ignore  # 2024-01-30 # TODO: Return type "Coroutine[Any, Any, list[BaseFileS3ConnTargetDTO]]" of "_make_target_conn_dto_pool" incompatible with return type "Coroutine[Any, Any, list[ClickHouseConnTargetDTO]]" in supertype "_BaseClickHouseConnExecutor"  [override]
        dto_pool = []
        for host in self._conn_hosts_pool:
            dto_pool.append(
                BaseFileS3ConnTargetDTO(
                    conn_id=self._conn_dto.conn_id,
                    pass_db_messages_to_user=self._conn_options.pass_db_messages_to_user,
                    pass_db_query_to_user=self._conn_options.pass_db_query_to_user,
                    protocol=self._conn_dto.protocol,
                    host=host,
                    port=self._conn_dto.port,
                    db_name=None,
                    username=self._conn_dto.username,
                    password=self._conn_dto.password,
                    max_execution_time=self._conn_options.max_execution_time,
                    total_timeout=self._conn_options.total_timeout,
                    connect_timeout=self._conn_options.connect_timeout,
                    disable_value_processing=self._conn_options.disable_value_processing,
                    s3_endpoint=self._conn_dto.s3_endpoint,
                    bucket=self._conn_dto.bucket,
                    access_key_id=self._conn_dto.access_key_id,
                    secret_access_key=self._conn_dto.secret_access_key,
                    replace_secret=self._conn_dto.replace_secret,
                    ca_data=self._ca_data.decode("ascii"),
                )
            )
        return dto_pool
