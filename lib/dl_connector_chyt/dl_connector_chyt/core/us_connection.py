from __future__ import annotations

import abc
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Optional,
)

import attr
import marshmallow as ma

from dl_constants.enums import DataSourceType
from dl_core.base_models import (
    ConnCacheableDataModelMixin,
    ConnSubselectDataModelMixin,
)
from dl_core.connection_executors.sync_base import SyncConnExecutorBase
from dl_core.us_connection_base import (
    ConnectionBase,
    DataSourceTemplate,
    SubselectMixin,
)
from dl_core.utils import secrepr
from dl_i18n.localizer_base import Localizer
from dl_utils.utils import DataKey

from dl_connector_chyt.api.i18n.localizer import Translatable
from dl_connector_chyt.core.conn_options import CHYTConnectOptions
from dl_connector_chyt.core.constants import (
    SOURCE_TYPE_CHYT_YTSAURUS_SUBSELECT,
    SOURCE_TYPE_CHYT_YTSAURUS_TABLE,
    SOURCE_TYPE_CHYT_YTSAURUS_TABLE_LIST,
    SOURCE_TYPE_CHYT_YTSAURUS_TABLE_RANGE,
)
from dl_connector_chyt.core.dto import CHYTDTO
from dl_connector_chyt.core.settings import CHYTConnectorSettings
from dl_connector_clickhouse.core.clickhouse_base.us_connection import ConnectionClickhouseBase


if TYPE_CHECKING:
    from dl_core.services_registry.top_level import ServicesRegistry


class BaseConnectionCHYT(
    SubselectMixin,
    ConnectionBase,
    abc.ABC,
):
    allow_dashsql: ClassVar[bool] = True
    is_always_user_source: ClassVar[bool] = True
    settings_type = CHYTConnectorSettings

    chyt_table_source_type: ClassVar[DataSourceType]
    chyt_table_list_source_type: ClassVar[DataSourceType]
    chyt_table_range_source_type: ClassVar[DataSourceType]
    chyt_subselect_source_type: ClassVar[DataSourceType]

    @attr.s(kw_only=True)
    class DataModel(ConnCacheableDataModelMixin, ConnSubselectDataModelMixin, ConnectionBase.DataModel):
        alias: str = attr.ib()
        max_execution_time: Optional[int] = attr.ib(default=None)

    @property
    def _connector_settings(self) -> CHYTConnectorSettings:
        settings = super()._connector_settings
        assert isinstance(settings, CHYTConnectorSettings)
        return settings

    async def validate_new_data(
        self,
        services_registry: ServicesRegistry,
        changes: Optional[dict] = None,
        original_version: Optional[ConnectionBase] = None,
    ) -> None:
        chyt_settings = self._connector_settings
        if self.data.alias in chyt_settings.FORBIDDEN_CLIQUES:
            err_msg = f'It is forbidden to use the following cliques: {", ".join(chyt_settings.FORBIDDEN_CLIQUES)}.'
            if chyt_settings.PUBLIC_CLIQUES:
                err_msg += f' Valid public cliques are: {", ".join(chyt_settings.PUBLIC_CLIQUES)}.'
            raise ma.ValidationError(message=err_msg)

    @property
    def cache_ttl_sec_override(self) -> Optional[int]:
        return self.data.cache_ttl_sec

    def get_conn_options(self) -> CHYTConnectOptions:
        base = super().get_conn_options()
        base_ch_connect_options = ConnectionClickhouseBase.get_effective_conn_options(
            base_conn_opts=base,
            user_max_execution_time=self.data.max_execution_time,
            max_allowed_max_execution_time=ConnectionClickhouseBase.MAX_ALLOWED_MAX_EXECUTION_TIME,
        )
        return base_ch_connect_options.to_subclass(CHYTConnectOptions)

    def get_data_source_templates(
        self,
        conn_executor_factory: Callable[[ConnectionBase], SyncConnExecutorBase],
    ) -> list[DataSourceTemplate]:
        return []

    def get_data_source_template_templates(self, localizer: Localizer) -> list[DataSourceTemplate]:
        common: dict[str, Any] = dict(
            group=[],
            connection_id=self.uuid,
            parameters={},
        )
        return [
            DataSourceTemplate(
                title="YTsaurus table via CHYT",
                tab_title=localizer.translate(Translatable("source_templates-tab_title-table")),
                source_type=self.chyt_table_source_type,
                form=[
                    {
                        "name": "table_name",
                        "input_type": "text",
                        "default": "",
                        "required": True,
                        "title": localizer.translate(Translatable("source_templates-label-ytsaurus_table")),
                        "field_doc_key": "YTsaurus/CHYT_TABLE/table_name",
                    },
                ],
                **common,
            ),
            DataSourceTemplate(
                title="List of YTsaurus tables via CHYT",
                tab_title=localizer.translate(Translatable("source_templates-tab_title-concat")),
                source_type=self.chyt_table_list_source_type,
                form=[
                    {
                        "name": "table_names",
                        "input_type": "textarea",
                        "default": "",
                        "required": True,
                        "title": localizer.translate(Translatable("source_templates-label-ytasurus_table_list")),
                        "field_doc_key": "YTsaurus/CHYT_TABLE_LIST/table_names",
                    },
                ],
                **common,
            ),
            DataSourceTemplate(
                title="Range of YTsaurus tables via CHYT",
                tab_title=localizer.translate(Translatable("source_templates-tab_title-range")),
                source_type=self.chyt_table_range_source_type,
                form=[
                    {
                        "name": "directory_path",
                        "input_type": "text",
                        "default": "",
                        "required": True,
                        "title": localizer.translate(Translatable("source_templates-label-ytsaurus_dir")),
                        "field_doc_key": "YTsaurus/CHYT_TABLE_RANGE/directory_path",
                    },
                    {
                        "name": "range_from",
                        "input_type": "text",
                        "default": "",
                        "required": False,
                        "title": localizer.translate(Translatable("source_templates-label-range_from")),
                    },
                    {
                        "name": "range_to",
                        "input_type": "text",
                        "default": "",
                        "required": False,
                        "title": localizer.translate(Translatable("source_templates-label-range_to")),
                    },
                ],
                **common,
            ),
        ] + self._make_subselect_templates(
            title="SQL query via CHYT",
            source_type=self.chyt_subselect_source_type,
            field_doc_key="YTsaurus/CHYT_SUBSELECT/subsql",
            localizer=localizer,
        )


class ConnectionCHYTToken(BaseConnectionCHYT):
    allow_cache: ClassVar[bool] = True

    source_type = SOURCE_TYPE_CHYT_YTSAURUS_TABLE
    allowed_source_types = frozenset(
        (
            SOURCE_TYPE_CHYT_YTSAURUS_TABLE,
            SOURCE_TYPE_CHYT_YTSAURUS_SUBSELECT,
            SOURCE_TYPE_CHYT_YTSAURUS_TABLE_LIST,
            SOURCE_TYPE_CHYT_YTSAURUS_TABLE_RANGE,
        )
    )

    chyt_table_source_type = SOURCE_TYPE_CHYT_YTSAURUS_TABLE
    chyt_table_list_source_type = SOURCE_TYPE_CHYT_YTSAURUS_TABLE_LIST
    chyt_table_range_source_type = SOURCE_TYPE_CHYT_YTSAURUS_TABLE_RANGE
    chyt_subselect_source_type = SOURCE_TYPE_CHYT_YTSAURUS_SUBSELECT

    @attr.s(kw_only=True)
    class DataModel(BaseConnectionCHYT.DataModel):
        host: str = attr.ib()
        port: int = attr.ib()
        token: str = attr.ib(repr=secrepr)
        secure: bool = attr.ib(default=False)

        @classmethod
        def get_secret_keys(cls) -> set[DataKey]:
            return {
                *super().get_secret_keys(),
                DataKey(parts=("token",)),
            }

    @property
    def token(self) -> str:
        return self.data.token

    def get_conn_dto(self) -> CHYTDTO:
        return CHYTDTO(
            conn_id=self.uuid,
            clique_alias=self.data.alias,
            token=self.token,
            host=self.data.host,
            port=self.data.port,
            protocol="https" if self.data.secure else "http",
        )
