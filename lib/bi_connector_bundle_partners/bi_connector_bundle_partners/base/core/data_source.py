from __future__ import annotations

import abc
from typing import (
    TYPE_CHECKING,
    ClassVar,
    Iterable,
)

from dl_connector_clickhouse.core.clickhouse_base.data_source import ClickHouseDataSourceBase
from dl_core import exc

if TYPE_CHECKING:
    from dl_core.base_models import SourceFilterSpec
    from dl_core.services_registry.top_level import ServicesRegistry


class PartnersCHDataSourceBase(ClickHouseDataSourceBase, metaclass=abc.ABCMeta):
    """
    Partners connector datasource with specific database for each client.
    """

    preview_enabled: ClassVar[bool] = True

    def get_filters(self, service_registry: ServicesRegistry) -> Iterable[SourceFilterSpec]:
        if self.db_name != self.connection.db_name:
            raise exc.SourceDoesNotExist(db_message="", query="")
        return []
