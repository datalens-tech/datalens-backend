from __future__ import annotations

from typing import Optional

import attr

from dl_constants.enums import (
    DataSourceRole,
    ManagedBy,
)
from dl_core.base_models import DefaultConnectionRef
from dl_core.data_source_spec.base import DataSourceSpec


@attr.s
class DataSourceCollectionSpec:  # noqa
    id: str = attr.ib(kw_only=True)
    title: Optional[str] = attr.ib(kw_only=True, default=None)
    managed_by: ManagedBy = attr.ib(kw_only=True, default=None)
    valid: bool = attr.ib(kw_only=True, default=True)

    origin: Optional[DataSourceSpec] = attr.ib(kw_only=True, default=None)
    materialization: Optional[DataSourceSpec] = attr.ib(kw_only=True, default=None)
    sample: Optional[DataSourceSpec] = attr.ib(kw_only=True, default=None)

    def __attrs_post_init__(self) -> None:
        self.managed_by = self.managed_by or ManagedBy.user

    def get_for_role(self, role: DataSourceRole) -> Optional[DataSourceSpec]:
        return getattr(self, role.name)

    def set_for_role(self, role: DataSourceRole, value: Optional[DataSourceSpec]) -> None:
        setattr(self, role.name, value)

    def has_role(self, role: DataSourceRole) -> bool:
        return bool(getattr(self, role.name, None))

    def collect_links(self) -> dict[str, str]:
        assert self.origin is not None
        result: dict[str, str] = {}
        connection_ref = self.origin.connection_ref
        if isinstance(connection_ref, DefaultConnectionRef):
            result[self.id] = connection_ref.conn_id
        return result
