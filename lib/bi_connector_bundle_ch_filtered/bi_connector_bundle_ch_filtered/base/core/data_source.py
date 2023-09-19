from __future__ import annotations

import abc
import logging
from typing import (
    TYPE_CHECKING,
    ClassVar,
    Iterable,
    Optional,
)

import sqlalchemy as sa

from dl_connector_clickhouse.core.clickhouse_base.data_source import (
    ActualClickHouseBaseMixin,
    ClickHouseDataSourceBase,
    CommonClickHouseSubselectDataSource,
)
from dl_core import exc
from dl_core.base_models import SourceFilterSpec
from dl_core.data_source_spec.sql import StandardSQLDataSourceSpec


if TYPE_CHECKING:
    from dl_core import us_connection  # noqa
    from dl_core.db import (  # noqa
        SchemaColumn,
        SchemaInfo,
    )
    from dl_core.services_registry import ServicesRegistry


LOGGER = logging.getLogger(__name__)


class ClickHouseFilteredDataSourceBase(ClickHouseDataSourceBase, metaclass=abc.ABCMeta):
    preview_enabled: ClassVar[bool] = False

    def _check_db_table_is_allowed(self) -> None:
        if self.db_name != self.connection.db_name or self.table_name not in self.connection.allowed_tables:
            raise exc.SourceDoesNotExist(db_message="", query="")

    def get_filters(self, service_registry: ServicesRegistry) -> Iterable[SourceFilterSpec]:
        self._check_db_table_is_allowed()
        return super().get_filters(service_registry)


class ClickHouseTemplatedSubselectDataSource(ActualClickHouseBaseMixin, CommonClickHouseSubselectDataSource):
    preview_enabled: ClassVar[bool] = True

    @property
    def spec(self) -> StandardSQLDataSourceSpec:  # type: ignore  # TODO: fix
        assert isinstance(self._spec, StandardSQLDataSourceSpec)
        return self._spec

    @property
    def subsql(self) -> Optional[str]:
        ss_template = self.connection.get_subselect_template_by_title(self.spec.table_name)
        parameters = self.connection.subselect_parameters
        LOGGER.debug("Got subselect parameters: %s", parameters)

        return str(
            sa.text(ss_template.sql_query)
            .bindparams(**{param.name: param.values for param in parameters})  # TODO: param_type = single_value
            .compile(dialect=self.connection.get_dialect(), compile_kwargs={"literal_binds": True})
        )

    def get_parameters(self) -> dict:
        return dict(
            db_name=self.spec.db_name,
            table_name=self.spec.table_name,
        )
