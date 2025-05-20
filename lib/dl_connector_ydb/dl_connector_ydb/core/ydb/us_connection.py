from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Callable,
    ClassVar,
    Optional,
)

import attr

from dl_core.connection_executors.sync_base import SyncConnExecutorBase
from dl_core.us_connection_base import (
    ClassicConnectionSQL,
    ConnectionBase,
    ConnectionSettingsMixin,
    DataSourceTemplate,
    make_subselect_datasource_template,
    make_table_datasource_template,
)
from dl_core.utils import secrepr
from dl_i18n.localizer_base import Localizer
from dl_utils.utils import DataKey

from dl_connector_ydb.api.ydb.i18n.localizer import Translatable
from dl_connector_ydb.core.ydb.constants import (
    SOURCE_TYPE_YDB_SUBSELECT,
    SOURCE_TYPE_YDB_TABLE,
    YDBAuthTypeMode,
)
from dl_connector_ydb.core.ydb.dto import YDBConnDTO
from dl_connector_ydb.core.ydb.settings import YDBConnectorSettings


if TYPE_CHECKING:
    from dl_core.connection_models.common_models import TableIdent


class YDBConnection(
    ConnectionSettingsMixin[YDBConnectorSettings],
    ClassicConnectionSQL,
):
    allow_cache: ClassVar[bool] = True
    is_always_user_source: ClassVar[bool] = True
    allow_dashsql: ClassVar[bool] = True
    allow_export: ClassVar[bool] = True
    settings_type = YDBConnectorSettings

    source_type = SOURCE_TYPE_YDB_TABLE

    @attr.s(kw_only=True)
    class DataModel(ClassicConnectionSQL.DataModel):
        auth_type: Optional[YDBAuthTypeMode] = attr.ib(default=YDBAuthTypeMode.oauth)
        username: Optional[str] = attr.ib(default="")

        token: Optional[str] = attr.ib(default=None, repr=secrepr)

        ssl_enable: bool = attr.ib(kw_only=True, default=False)
        ssl_ca: Optional[str] = attr.ib(kw_only=True, default=None)

        @classmethod
        def get_secret_keys(cls) -> set[DataKey]:
            return {
                *super().get_secret_keys(),
                DataKey(parts=("token",)),
            }

    def get_conn_dto(self) -> YDBConnDTO:
        assert self.data.db_name
        return YDBConnDTO(
            conn_id=self.uuid,
            host=self.data.host,
            multihosts=(),
            port=self.data.port,
            db_name=self.data.db_name,
            username=self.data.username,
            password=self.data.token,
            auth_type=self.data.auth_type,
            ssl_enable=self.data.ssl_enable,
            ssl_ca=self.data.ssl_ca,
        )

    def get_data_source_template_templates(self, localizer: Localizer) -> list[DataSourceTemplate]:
        return [
            make_table_datasource_template(
                connection_id=self.uuid,  # type: ignore
                source_type=SOURCE_TYPE_YDB_TABLE,
                localizer=localizer,
                disabled=not self.is_datasource_template_allowed,
                form_title=localizer.translate(Translatable("source_templates-label-ydb_table")),
                field_doc_key="YDB_TABLE/table_name",
                template_enabled=self.is_datasource_template_allowed,
            ),
            make_subselect_datasource_template(
                connection_id=self.uuid,  # type: ignore
                source_type=SOURCE_TYPE_YDB_SUBSELECT,
                localizer=localizer,
                disabled=not self.is_subselect_allowed,
                title="Subselect over YDB",
                template_enabled=self.is_datasource_template_allowed,
            ),
        ]

    def get_tables(
        self,
        conn_executor_factory: Callable[[ConnectionBase], SyncConnExecutorBase],
        db_name: Optional[str] = None,
        schema_name: Optional[str] = None,
    ) -> list[TableIdent]:
        if db_name is None:
            # Only current-database listing is feasible here.
            db_name = self.data.db_name
        return super().get_tables(
            conn_executor_factory=conn_executor_factory,
            db_name=db_name,
            schema_name=schema_name,
        )

    @property
    def allow_public_usage(self) -> bool:
        return True

    @property
    def is_datasource_template_allowed(self) -> bool:
        return self._connector_settings.ENABLE_DATASOURCE_TEMPLATE and super().is_datasource_template_allowed
