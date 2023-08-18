from __future__ import annotations

from typing import Any, Callable, ClassVar, Optional

import attr

from bi_constants.enums import CreateDSFrom
from bi_utils.utils import DataKey

from bi_core.connectors.chydb.dto import CHOverYDBDTO
from bi_core.connectors.clickhouse_base.us_connection import ConnectionClickhouseBase
from bi_core.i18n.localizer import Translatable
from bi_core.i18n.localizer_base import Localizer
from bi_core.connection_executors.sync_base import SyncConnExecutorBase
from bi_core.us_connection_base import DataSourceTemplate, ConnectionBase
from bi_core.utils import secrepr


class ConnectionCHYDB(ConnectionClickhouseBase):
    """
    Clickhouse,
    without table-listing (CHYT-like 'enter table name'),
    and other minor customizations.
    """

    source_type = CreateDSFrom.CHYDB_TABLE
    allowed_source_types = frozenset((CreateDSFrom.CHYDB_TABLE, CreateDSFrom.CHYDB_SUBSELECT))
    allow_dashsql: ClassVar[bool] = True
    allow_cache: ClassVar[bool] = True
    is_always_user_source: ClassVar[bool] = True

    @attr.s(kw_only=True)
    class DataModel(ConnectionClickhouseBase.DataModel):
        db_name = None  # type: ignore  # always 'default'
        username = None  # type: ignore  # always 'default'
        password = None  # type: ignore  # -> 'token'
        cluster_name = None  # type: ignore  # not useful.

        token: str = attr.ib(repr=secrepr)

        default_ydb_cluster: Optional[str] = attr.ib(default=None)
        default_ydb_database: Optional[str] = attr.ib(default=None)

        @classmethod
        def get_secret_keys(cls) -> set[DataKey]:
            return {
                *super().get_secret_keys(),
                DataKey(parts=('token',)),
            }

    @property
    def token(self):  # type: ignore  # TODO: fix
        return self.data.token

    def get_conn_dto(self) -> CHOverYDBDTO:  # type: ignore  # TODO: fix
        return CHOverYDBDTO(
            conn_id=self.uuid,
            protocol='https' if self.data.secure else 'http',
            host=self.data.host,
            port=self.data.port,
            endpoint=self.data.endpoint,
            token=self.token,
        )

    def get_data_source_template_templates(self, localizer: Localizer) -> list[DataSourceTemplate]:
        common: dict[str, Any] = dict(
            group=[],
            connection_id=self.uuid,
            is_ref=False,
            ref_source_id=None,
        )
        return [
            DataSourceTemplate(
                title='CH over YDB',
                tab_title=localizer.translate(Translatable('source_templates-tab_title-table')),
                source_type=CreateDSFrom.CHYDB_TABLE,
                parameters=dict(
                    ydb_cluster=self.data.default_ydb_cluster,
                    ydb_database=self.data.default_ydb_database,
                ),
                form=[
                    {
                        "name": "table_name", "input_type": "text",
                        "default": "", "required": True,
                        "title": localizer.translate(Translatable('source_templates-label-ydb_table')),
                        "field_doc_key": "CHYDB_TABLE/table_name",
                    },
                    {
                        "name": "ydb_database", "input_type": "text",
                        "default": self.data.default_ydb_database, "required": True,
                        "title": localizer.translate(Translatable('source_templates-label-ydb_database')),
                    },
                    {
                        "name": "ydb_cluster", "input_type": "select",
                        "default": self.data.default_ydb_cluster, "required": True,
                        "select_options": ["ru", "ru-prestable", "eu"],
                        "select_allow_user_input": False,
                        "title": localizer.translate(Translatable('source_templates-label-ydb_cluster')),
                    },
                ],
                **common,
            ),
        ] + self._make_subselect_templates(
            title='CH over subselect over YDB',
            source_type=CreateDSFrom.CHYDB_SUBSELECT,
            localizer=localizer,
        )

    def get_data_source_templates(
            self, conn_executor_factory: Callable[[ConnectionBase], SyncConnExecutorBase],
    ) -> list[DataSourceTemplate]:
        return []

    @property
    def db_name(self) -> Optional[str]:
        return 'default'

    @property
    def allow_public_usage(self) -> bool:
        return True
