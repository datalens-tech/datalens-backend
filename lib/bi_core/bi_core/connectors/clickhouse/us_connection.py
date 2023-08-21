from __future__ import annotations

from typing import ClassVar

from bi_constants.enums import CreateDSFrom

from bi_core.connectors.clickhouse_base.us_connection import ConnectionClickhouseBase
from bi_i18n.localizer_base import Localizer
from bi_core.us_connection_base import DataSourceTemplate


class ConnectionClickhouse(ConnectionClickhouseBase):
    """
    User's ClickHouse database.
    Should not be used for internal clickhouses.
    """

    source_type = CreateDSFrom.CH_TABLE
    allowed_source_types = frozenset((CreateDSFrom.CH_TABLE, CreateDSFrom.CH_SUBSELECT))
    allow_dashsql: ClassVar[bool] = True
    allow_cache: ClassVar[bool] = True
    is_always_user_source: ClassVar[bool] = False  # TODO: should be `True`, but need some cleanup for that.

    def get_data_source_template_templates(self, localizer: Localizer) -> list[DataSourceTemplate]:
        return self._make_subselect_templates(source_type=CreateDSFrom.CH_SUBSELECT, localizer=localizer)

    @property
    def allow_public_usage(self) -> bool:
        return True
