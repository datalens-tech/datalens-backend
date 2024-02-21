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
    DataSourceTemplate,
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


if TYPE_CHECKING:
    from dl_core.connection_models.common_models import TableIdent


class YDBConnection(ClassicConnectionSQL):
    allow_cache: ClassVar[bool] = True
    is_always_user_source: ClassVar[bool] = True
    allow_dashsql: ClassVar[bool] = True

    source_type = SOURCE_TYPE_YDB_TABLE

    @attr.s(kw_only=True)
    class DataModel(ClassicConnectionSQL.DataModel):
        auth_type: Optional[str] = attr.ib(default=YDBAuthTypeMode.oauth.value)
        username: Optional[str] = attr.ib(default="")

        token: Optional[str] = attr.ib(default=None, repr=secrepr)

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
        )

    def get_data_source_template_templates(self, localizer: Localizer) -> list[DataSourceTemplate]:
        return [
            DataSourceTemplate(
                title="YDB table",
                tab_title=localizer.translate(Translatable("source_templates-tab_title-table")),
                source_type=SOURCE_TYPE_YDB_TABLE,
                parameters=dict(),
                form=[
                    {
                        "name": "table_name",
                        "input_type": "text",
                        "default": "",
                        "required": True,
                        "title": localizer.translate(Translatable("source_templates-label-ydb_table")),
                        "field_doc_key": "YDB_TABLE/table_name",
                    },
                ],
                group=[],
                connection_id=self.uuid,  # type: ignore  # TODO: fix
            ),
        ] + self._make_subselect_templates(
            title="Subselect over YDB",
            source_type=SOURCE_TYPE_YDB_SUBSELECT,
            localizer=localizer,
        )

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
