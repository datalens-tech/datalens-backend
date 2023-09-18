from __future__ import annotations

from typing import ClassVar

from dl_connector_clickhouse.core.clickhouse.constants import (
    SOURCE_TYPE_CH_SUBSELECT,
    SOURCE_TYPE_CH_TABLE,
)
from dl_connector_clickhouse.core.clickhouse_base.us_connection import ConnectionClickhouseBase
from dl_core.us_connection_base import DataSourceTemplate
from dl_i18n.localizer_base import Localizer


class ConnectionClickhouse(ConnectionClickhouseBase):
    """
    User's ClickHouse database.
    Should not be used for internal clickhouses.
    """

    source_type = SOURCE_TYPE_CH_TABLE
    allowed_source_types = frozenset((SOURCE_TYPE_CH_TABLE, SOURCE_TYPE_CH_SUBSELECT))
    allow_dashsql: ClassVar[bool] = True
    allow_cache: ClassVar[bool] = True
    is_always_user_source: ClassVar[bool] = False  # TODO: should be `True`, but need some cleanup for that.

    def get_data_source_template_templates(self, localizer: Localizer) -> list[DataSourceTemplate]:
        return self._make_subselect_templates(source_type=SOURCE_TYPE_CH_SUBSELECT, localizer=localizer)

    @property
    def allow_public_usage(self) -> bool:
        return True
