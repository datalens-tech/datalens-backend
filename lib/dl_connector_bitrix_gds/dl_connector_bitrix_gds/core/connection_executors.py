from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Optional,
    Sequence,
)

import attr

from dl_core.aio.web_app_services.redis import RedisConnParams
from dl_core.connection_executors.async_sa_executors import DefaultSqlAlchemyConnExecutor

from dl_connector_bitrix_gds.core.adapter import BitrixGDSDefaultAdapter
from dl_connector_bitrix_gds.core.target_dto import BitrixGDSConnTargetDTO


if TYPE_CHECKING:
    from dl_connector_bitrix_gds.core.dto import BitrixGDSConnDTO


@attr.s(cmp=False, hash=False)
class BitrixGDSAsyncAdapterConnExecutor(DefaultSqlAlchemyConnExecutor[BitrixGDSDefaultAdapter]):
    TARGET_ADAPTER_CLS = BitrixGDSDefaultAdapter

    _conn_dto: BitrixGDSConnDTO = attr.ib()

    async def _make_target_conn_dto_pool(self) -> Sequence[BitrixGDSConnTargetDTO]:
        redis_conn_params: Optional[RedisConnParams] = None
        caches_ttl: Optional[int] = None

        assert self._services_registry is not None
        rqe_caches_setting = self._services_registry.get_rqe_caches_settings()
        if rqe_caches_setting is not None:
            assert rqe_caches_setting.redis_settings is not None
            redis_conn_params = RedisConnParams(
                host=rqe_caches_setting.redis_settings.HOSTS[0],
                port=rqe_caches_setting.redis_settings.PORT,
                db=rqe_caches_setting.redis_settings.DB,
                password=rqe_caches_setting.redis_settings.PASSWORD,
                ssl=rqe_caches_setting.redis_settings.SSL,
            )
            caches_ttl = rqe_caches_setting.caches_ttl

        conn_params: Optional[dict]
        if isinstance(redis_conn_params, RedisConnParams):
            conn_params = attr.asdict(redis_conn_params)
        else:
            conn_params = None
        return [
            BitrixGDSConnTargetDTO(
                conn_id=self._conn_dto.conn_id,
                pass_db_messages_to_user=self._conn_options.pass_db_messages_to_user,
                pass_db_query_to_user=self._conn_options.pass_db_query_to_user,
                portal=self._conn_dto.portal,
                token=self._conn_dto.token,
                max_execution_time=self._conn_options.max_execution_time,  # type: ignore  # TODO: fix
                total_timeout=self._conn_options.total_timeout,  # type: ignore  # TODO: fix
                connect_timeout=self._conn_options.connect_timeout,  # type: ignore  # TODO: fix
                redis_conn_params=conn_params,
                redis_caches_ttl=caches_ttl,
            )
        ]
