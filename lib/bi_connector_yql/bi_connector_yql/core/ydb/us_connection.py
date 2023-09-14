from __future__ import annotations

from typing import TYPE_CHECKING, Callable, ClassVar, Optional

import attr

from bi_utils.utils import DataKey

from bi_connector_mdb_base.core.base_models import ConnMDBDataModelMixin
from bi_core.connection_models.conn_options import ConnectOptions
from bi_connector_yql.core.ydb.dto import YDBConnDTO
from bi_connector_yql.core.yql_base.us_connection import YQLConnectionMixin
from bi_core.i18n.localizer import Translatable
from bi_i18n.localizer_base import Localizer
from bi_core.connection_executors.sync_base import SyncConnExecutorBase
from bi_core.us_connection_base import ClassicConnectionSQL, DataSourceTemplate, ConnectionBase
from bi_core.utils import secrepr

from bi_connector_yql.core.ydb.constants import SOURCE_TYPE_YDB_TABLE, SOURCE_TYPE_YDB_SUBSELECT

if TYPE_CHECKING:
    from bi_core.connection_models.common_models import TableIdent


# TODO: remove
@attr.s(frozen=True, hash=True)
class YDBConnectOptions(ConnectOptions):
    is_cloud: bool = attr.ib(default=False)


class YDBConnection(YQLConnectionMixin, ClassicConnectionSQL):
    allow_cache: ClassVar[bool] = True
    is_always_user_source: ClassVar[bool] = True
    allow_dashsql: ClassVar[bool] = True

    source_type = SOURCE_TYPE_YDB_TABLE

    @attr.s(kw_only=True)
    class DataModel(ConnMDBDataModelMixin, ClassicConnectionSQL.DataModel):
        token: Optional[str] = attr.ib(default=None, repr=secrepr)
        service_account_id: Optional[str] = attr.ib(default=None)
        folder_id: Optional[str] = attr.ib(default=None)

        username = None  # type: ignore  # not applicable
        password = None  # type: ignore  # -> 'token'

        @classmethod
        def get_secret_keys(cls) -> set[DataKey]:
            return {
                *super().get_secret_keys(),
                DataKey(parts=('token',)),
            }

    def get_conn_options(self) -> YDBConnectOptions:
        return super().get_conn_options().to_subclass(
            YDBConnectOptions,
        )

    def get_conn_dto(self) -> YDBConnDTO:
        assert self.data.db_name
        return YDBConnDTO(
            conn_id=self.uuid,
            host=self.data.host,
            multihosts=(),
            port=self.data.port,
            db_name=self.data.db_name,
            username="",  # not applicable
            service_account_id=self.data.service_account_id,
            folder_id=self.data.folder_id,
            password=self.data.token,
        )

    def get_data_source_template_templates(self, localizer: Localizer) -> list[DataSourceTemplate]:
        return [
            DataSourceTemplate(
                title='YDB table',
                tab_title=localizer.translate(Translatable('source_templates-tab_title-table')),
                source_type=SOURCE_TYPE_YDB_TABLE,
                parameters=dict(),
                form=[
                    {
                        "name": "table_name", "input_type": "text",
                        "default": "", "required": True,
                        "title": localizer.translate(Translatable('source_templates-label-ydb_table')),
                        "field_doc_key": "YDB_TABLE/table_name",
                    },
                ],
                group=[],
                connection_id=self.uuid,  # type: ignore  # TODO: fix
            ),
        ] + self._make_subselect_templates(
            title='Subselect over YDB',
            source_type=SOURCE_TYPE_YDB_SUBSELECT,
            localizer=localizer,
        )

    def get_tables(
            self, conn_executor_factory: Callable[[ConnectionBase], SyncConnExecutorBase],
            db_name: Optional[str] = None, schema_name: Optional[str] = None,
    ) -> list[TableIdent]:
        if db_name is None:
            # Only current-database listing is feasible here.
            db_name = self.data.db_name
        return super().get_tables(
            conn_executor_factory=conn_executor_factory,
            db_name=db_name, schema_name=schema_name,
        )

    @property
    def allow_public_usage(self) -> bool:
        return True
