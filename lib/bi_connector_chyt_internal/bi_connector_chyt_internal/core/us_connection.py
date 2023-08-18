from __future__ import annotations

from typing import Optional, ClassVar, Any

import attr

from bi_utils.utils import DataKey

from bi_constants.enums import CreateDSFrom
from bi_connector_chyt.core.us_connection import BaseConnectionCHYT
from bi_connector_chyt_internal.core.conn_options import CHYTInternalConnectOptions
from bi_connector_chyt_internal.core.dto import CHYTInternalDTO, CHYTUserAuthDTO
from bi_core.i18n.localizer import Translatable
from bi_core.i18n.localizer_base import Localizer
from bi_core.us_connection_base import DataSourceTemplate
from bi_core.utils import secrepr


class BaseConnectionCHYTInternal(BaseConnectionCHYT):
    @attr.s(kw_only=True)
    class DataModel(BaseConnectionCHYT.DataModel):
        cluster: str = attr.ib()

        # deprecated, removed from storage schemas
        pool: Optional[str] = attr.ib(default=None)
        instance_count: Optional[int] = attr.ib(default=None)

    def get_conn_options(self) -> CHYTInternalConnectOptions:
        base_chyt_connect_options = super().get_conn_options()
        return base_chyt_connect_options.to_subclass(
            CHYTInternalConnectOptions,
            mirroring_clique_alias=None,
        )

    _table_source_field_doc_key: ClassVar[str] = "CHYT_TABLE/table_name"

    def get_data_source_template_templates(self, localizer: Localizer) -> list[DataSourceTemplate]:
        common: dict[str, Any] = dict(
            group=[],
            connection_id=self.uuid,
            parameters={},
        )
        return [
            DataSourceTemplate(
                title='CH over YT',
                tab_title=localizer.translate(Translatable('source_templates-tab_title-table')),
                source_type=self.chyt_table_source_type,
                form=[
                    {
                        "name": "table_name", "input_type": "text",
                        "default": "", "required": True,
                        "title": localizer.translate(Translatable('source_templates-label-yt_table')),
                        "field_doc_key": self._table_source_field_doc_key,
                    },
                ],
                **common,
            ),
            DataSourceTemplate(
                title='CH over a list of YT tables',
                tab_title=localizer.translate(Translatable('source_templates-tab_title-concat')),
                source_type=self.chyt_table_list_source_type,
                form=[
                    {
                        "name": "table_names", "input_type": "textarea",
                        "default": "", "required": True,
                        "title": localizer.translate(Translatable('source_templates-label-yt_table_list')),
                        "field_doc_key": "CHYT_TABLE_LIST/table_names",
                    },
                ],
                **common,
            ),
            DataSourceTemplate(
                title='CH over a range of YT tables',
                tab_title=localizer.translate(Translatable('source_templates-tab_title-range')),
                source_type=self.chyt_table_range_source_type,
                form=[
                    {
                        "name": "directory_path", "input_type": "text",
                        "default": "", "required": True,
                        "title": localizer.translate(Translatable('source_templates-label-yt_dir')),
                        "field_doc_key": "CHYT_TABLE_RANGE/directory_path",
                    },
                    {
                        "name": "range_from", "input_type": "text",
                        "default": "", "required": False,
                        "title": localizer.translate(Translatable('source_templates-label-range_from')),
                    },
                    {
                        "name": "range_to", "input_type": "text",
                        "default": "", "required": False,
                        "title": localizer.translate(Translatable('source_templates-label-range_to')),
                    },
                ],
                **common,
            ),
        ] + self._make_subselect_templates(
            title='CH over subselect over YT',
            source_type=self.chyt_subselect_source_type,
            field_doc_key='CHYT_SUBSELECT/subsql',
            localizer=localizer,
        )


class ConnectionCHYTInternalToken(BaseConnectionCHYTInternal):
    allow_cache: ClassVar[bool] = True

    source_type = CreateDSFrom.CHYT_TABLE  # Essentially, 'default source type', for `replace_connection`.
    allowed_source_types = frozenset((
        CreateDSFrom.CHYT_TABLE,
        CreateDSFrom.CHYT_TABLE_LIST,
        CreateDSFrom.CHYT_TABLE_RANGE,
        CreateDSFrom.CHYT_SUBSELECT,
    ))

    chyt_table_source_type = CreateDSFrom.CHYT_TABLE
    chyt_table_list_source_type = CreateDSFrom.CHYT_TABLE_LIST
    chyt_table_range_source_type = CreateDSFrom.CHYT_TABLE_RANGE
    chyt_subselect_source_type = CreateDSFrom.CHYT_SUBSELECT

    @attr.s(kw_only=True)
    class DataModel(BaseConnectionCHYTInternal.DataModel):
        token: str = attr.ib(repr=secrepr)

        @classmethod
        def get_secret_keys(cls) -> set[DataKey]:
            return {
                *super().get_secret_keys(),
                DataKey(parts=('token',)),
            }

    @property
    def token(self) -> str:
        return self.data.token

    def get_conn_dto(self) -> CHYTInternalDTO:
        return CHYTInternalDTO(
            conn_id=self.uuid,
            clique_alias=self.data.alias,
            cluster=self.data.cluster,
            token=self.token,
        )


class ConnectionCHYTUserAuth(BaseConnectionCHYTInternal):
    allow_cache: ClassVar[bool] = False  # Just to make this explicit.

    source_type = CreateDSFrom.CHYT_USER_AUTH_TABLE  # Essentially, 'default source type', for `replace_connection`.
    allowed_source_types = frozenset((
        CreateDSFrom.CHYT_USER_AUTH_TABLE,
        CreateDSFrom.CHYT_USER_AUTH_TABLE_LIST,
        CreateDSFrom.CHYT_USER_AUTH_TABLE_RANGE,
        CreateDSFrom.CHYT_USER_AUTH_SUBSELECT,
    ))

    chyt_table_source_type = CreateDSFrom.CHYT_USER_AUTH_TABLE
    chyt_table_list_source_type = CreateDSFrom.CHYT_USER_AUTH_TABLE_LIST
    chyt_table_range_source_type = CreateDSFrom.CHYT_USER_AUTH_TABLE_RANGE
    chyt_subselect_source_type = CreateDSFrom.CHYT_USER_AUTH_SUBSELECT
    _table_source_field_doc_key: ClassVar[str] = "CHYT_USER_AUTH_TABLE/table_name"

    @attr.s(kw_only=True)
    class DataModel(BaseConnectionCHYTInternal.DataModel):
        pass

    def __init__(self, *args, **kwargs):  # type: ignore  # TODO: fix
        super().__init__(*args, **kwargs)
        if not self._context:
            raise Exception("ConnectionClickhouseOverYTUserAuth._context is required")

    def get_conn_dto(self) -> CHYTUserAuthDTO:
        return CHYTUserAuthDTO(
            conn_id=self.uuid,
            clique_alias=self.data.alias,
            cluster=self.data.cluster,
            header_authorization=self._context.secret_headers.get("Authorization"),
            header_cookie=self._context.secret_headers.get("Cookie"),
        )
