from __future__ import annotations

import re
from datetime import datetime
from typing import Callable, ClassVar, Optional

import attr
import marshmallow as ma

from bi_api_commons.reporting.models import NotificationReportingRecord
from bi_connector_snowflake.auth import SFAuthProvider
from bi_connector_snowflake.core.constants import ACCOUNT_NAME_RE, NOTIF_TYPE_SF_REFRESH_TOKEN_SOON_TO_EXPIRE
from bi_connector_snowflake.core.constants import (
    CONNECTION_TYPE_SNOWFLAKE,
    SOURCE_TYPE_SNOWFLAKE_TABLE, SOURCE_TYPE_SNOWFLAKE_SUBSELECT,
)
from bi_connector_snowflake.core.dto import SnowFlakeConnDTO
from bi_core.base_models import (
    ConnectionDataModelBase,
    ConnCacheableMixin,
    ConnSubselectMixin,
)
from bi_core.reporting.notifications import get_notification_record
from bi_i18n.localizer_base import Localizer
from bi_core.connection_executors.sync_base import SyncConnExecutorBase
from bi_core.us_connection_base import ConnectionSQL, DataSourceTemplate, ConnectionBase
from bi_utils.utils import DataKey


class ConnectionSQLSnowFlake(ConnectionSQL):
    conn_type = CONNECTION_TYPE_SNOWFLAKE
    has_schema: ClassVar[bool] = True
    default_schema_name = None
    source_type = SOURCE_TYPE_SNOWFLAKE_TABLE
    allowed_source_types = frozenset((SOURCE_TYPE_SNOWFLAKE_TABLE, SOURCE_TYPE_SNOWFLAKE_SUBSELECT))
    allow_dashsql: ClassVar[bool] = False
    allow_cache: ClassVar[bool] = True
    is_always_user_source: ClassVar[bool] = True

    @attr.s(kw_only=True)
    class DataModel(ConnCacheableMixin, ConnSubselectMixin, ConnectionDataModelBase):
        account_name: str = attr.ib()
        user_name: str = attr.ib()
        user_role: Optional[str] = attr.ib(default=None)
        client_id: str = attr.ib()
        client_secret: str = attr.ib()
        refresh_token: str = attr.ib()
        refresh_token_expire_time: Optional[datetime] = attr.ib(default=None)

        schema: Optional[str] = attr.ib()
        db_name: Optional[str] = attr.ib()
        warehouse: Optional[str] = attr.ib()

        @classmethod
        def get_secret_keys(cls) -> set[DataKey]:
            return {DataKey(parts=("client_secret",)), DataKey(parts=("refresh_token",))}

    async def validate_new_data(
            self,
            changes: Optional[dict] = None,
            original_version: Optional[ConnectionBase] = None,
    ) -> None:
        if not re.fullmatch(ACCOUNT_NAME_RE, self.data.account_name):
            raise ma.ValidationError(message="Field account_name is not valid")

    def check_for_notifications(self) -> list[Optional[NotificationReportingRecord]]:
        notifications = super().check_for_notifications()

        sf_auth_provider = SFAuthProvider.from_dto(self.get_conn_dto())
        if sf_auth_provider.should_notify_refresh_token_to_expire_soon():
            notifications.append(get_notification_record(NOTIF_TYPE_SF_REFRESH_TOKEN_SOON_TO_EXPIRE))

        return notifications

    @property
    def schema_name(self) -> Optional[str]:
        return self.data.schema

    def get_conn_dto(self) -> SnowFlakeConnDTO:
        return SnowFlakeConnDTO(
            conn_id=self.uuid,
            account_name=self.data.account_name,
            user_name=self.data.user_name,
            user_role=self.data.user_role,
            client_id=self.data.client_id,
            client_secret=self.data.client_secret,
            refresh_token=self.data.refresh_token,
            refresh_token_expire_time=self.data.refresh_token_expire_time,
            schema=self.data.schema,
            db_name=self.data.db_name,
            warehouse=self.data.warehouse,
        )

    def get_parameter_combinations(
            self, conn_executor_factory: Callable[[ConnectionBase], SyncConnExecutorBase],
    ) -> list[dict]:
        # todo: for initial implementation using only schema and db name from conn dto
        tables = self.get_tables(
            conn_executor_factory=conn_executor_factory,
            schema_name=self.data.schema, db_name=self.data.db_name,
        )
        return [dict(schema_name=tid.schema_name, table_name=tid.table_name) for tid in tables]

    def get_data_source_template_group(self, parameters: dict) -> list[str]:
        return [val for val in (
            parameters.get('account_name'),
            parameters.get('user_name'),
            parameters.get('user_role'),
            parameters.get('client_id'),
            parameters.get('client_secret'),
            parameters.get('refresh_token'),
            parameters.get('refresh_token_expire_time'),
            parameters.get('schema'),
            parameters.get('db_name'),
            parameters.get('warehouse'),
        ) if val is not None]

    @property
    def allow_public_usage(self) -> bool:
        return False

    def get_data_source_template_templates(self, localizer: Localizer) -> list[DataSourceTemplate]:
        return self._make_subselect_templates(source_type=SOURCE_TYPE_SNOWFLAKE_SUBSELECT, localizer=localizer)
