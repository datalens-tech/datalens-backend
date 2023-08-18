from __future__ import annotations

from typing import Callable, ClassVar, Optional

import attr

from bi_configs.connectors_settings import YQConnectorSettings
from bi_utils.utils import DataKey

from bi_core.base_models import ConnCacheableMixin, ConnSubselectMixin
from bi_core.connection_models.conn_options import ConnectOptions
from bi_core.i18n.localizer_base import Localizer
from bi_core.connection_executors.sync_base import SyncConnExecutorBase
from bi_core.us_connection_base import (
    ConnectionBase, DataSourceTemplate, ExecutorBasedMixin, SubselectMixin, ConnectionHardcodedDataMixin,
)
from bi_core.utils import secrepr

from bi_connector_yql.core.yq.constants import SOURCE_TYPE_YQ_TABLE, SOURCE_TYPE_YQ_SUBSELECT
from bi_connector_yql.core.yq.dto import YQConnDTO
from bi_connector_yql.core.yql_base.us_connection import YQLConnectionMixin


# TODO: remove
@attr.s(frozen=True, hash=True)
class YQConnectOptions(ConnectOptions):
    pass


class YQConnection(
        ConnectionHardcodedDataMixin[YQConnectorSettings],
        YQLConnectionMixin,
        SubselectMixin,
        ExecutorBasedMixin,
        ConnectionBase,
):
    allow_cache: ClassVar[bool] = True
    is_always_user_source: ClassVar[bool] = True
    allow_dashsql: ClassVar[bool] = True

    source_type = SOURCE_TYPE_YQ_TABLE

    @attr.s(kw_only=True)
    class DataModel(ConnCacheableMixin, ConnSubselectMixin, ConnectionBase.DataModel):
        service_account_id: Optional[str] = attr.ib(default=None)
        folder_id: Optional[str] = attr.ib(default=None)

        # More "authentication_data" than "password", but naming it "password" for compatibility.
        # Only the user authentication (SA key data) is required.
        password: str = attr.ib(default='', repr=secrepr)

        @classmethod
        def get_secret_keys(cls) -> set[DataKey]:
            return {
                *super().get_secret_keys(),
                DataKey(parts=('password',)),
            }

    @property
    def _connector_settings(self) -> YQConnectorSettings:
        settings = self._all_connectors_settings.YQ
        assert settings is not None
        return settings

    def get_conn_options(self) -> YQConnectOptions:
        return super().get_conn_options().to_subclass(
            YQConnectOptions,
        )

    def get_conn_dto(self) -> YQConnDTO:
        cs = self._connector_settings
        return YQConnDTO(
            conn_id=self.uuid,
            service_account_id=self.data.service_account_id,
            folder_id=self.data.folder_id,
            password=self.data.password,
            host=cs.HOST,
            port=cs.PORT,
            db_name=cs.DB_NAME,
        )

    @property
    def cache_ttl_sec_override(self) -> Optional[int]:
        return self.data.cache_ttl_sec

    def get_data_source_template_templates(self, localizer: Localizer) -> list[DataSourceTemplate]:
        return self._make_subselect_templates(
            title='Subselect over YQ',
            source_type=SOURCE_TYPE_YQ_SUBSELECT,
            localizer=localizer,
        )

    def get_data_source_templates(
            self, conn_executor_factory: Callable[[ConnectionBase], SyncConnExecutorBase]
    ) -> list[DataSourceTemplate]:
        return []  # TODO.

    @property
    def allow_public_usage(self) -> bool:
        return True
