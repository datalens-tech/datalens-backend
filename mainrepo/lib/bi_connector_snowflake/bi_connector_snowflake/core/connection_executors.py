from __future__ import annotations

import logging
from typing import TypeVar

import attr

from bi_core.connection_executors.adapters.common_base import CommonBaseDirectAdapter
from bi_core.connection_executors.async_sa_executors import DefaultSqlAlchemyConnExecutor
from bi_core.reporting.notifications import get_notification_record

from bi_connector_snowflake.core.constants import NOTIF_TYPE_SF_REFRESH_TOKEN_SOON_TO_EXPIRE
from bi_connector_snowflake.auth import SFAuthProvider
from bi_connector_snowflake.core.adapters import SnowFlakeDefaultAdapter
from bi_connector_snowflake.core.dto import SnowFlakeConnDTO
from bi_connector_snowflake.core.exc import SnowflakeRefreshTokenInvalid
from bi_connector_snowflake.core.target_dto import SnowFlakeConnTargetDTO


LOGGER = logging.getLogger(__name__)


_BASE_SNOWFLAKE_ADAPTER_TV = TypeVar("_BASE_SNOWFLAKE_ADAPTER_TV", bound=CommonBaseDirectAdapter)


@attr.s(cmp=False, hash=False)
class BaseSnowFlakeConnExecutor(DefaultSqlAlchemyConnExecutor[_BASE_SNOWFLAKE_ADAPTER_TV]):
    _conn_dto: SnowFlakeConnDTO = attr.ib()

    async def _make_target_conn_dto_pool(self) -> list[SnowFlakeConnTargetDTO]:  # type: ignore  # TODO: fix
        sf_auth_provider = SFAuthProvider.from_dto(self._conn_dto)

        if sf_auth_provider.is_refresh_token_expired():
            raise SnowflakeRefreshTokenInvalid()
        elif sf_auth_provider.should_notify_refresh_token_to_expire_soon():
            reporting_service = None
            if self._services_registry is not None:
                try:
                    reporting_service = self._services_registry.get_reporting_registry()
                except AttributeError:
                    LOGGER.error("Failed to get reporting registry from service registry")

            if reporting_service:
                reporting_service.save_reporting_record(
                    get_notification_record(NOTIF_TYPE_SF_REFRESH_TOKEN_SOON_TO_EXPIRE)
                )

        access_token = await sf_auth_provider.async_get_access_token()

        return [
            SnowFlakeConnTargetDTO(
                conn_id=self._conn_dto.conn_id,
                pass_db_messages_to_user=self._conn_options.pass_db_messages_to_user,
                pass_db_query_to_user=self._conn_options.pass_db_query_to_user,
                account_name=self._conn_dto.account_name,
                user_name=self._conn_dto.user_name,
                user_role=self._conn_dto.user_role,
                schema=self._conn_dto.schema,
                db_name=self._conn_dto.db_name,
                warehouse=self._conn_dto.warehouse,
                access_token=access_token,
            )
        ]


@attr.s(cmp=False, hash=False)
class SnowFlakeSyncConnExecutor(BaseSnowFlakeConnExecutor[SnowFlakeDefaultAdapter]):
    TARGET_ADAPTER_CLS = SnowFlakeDefaultAdapter
