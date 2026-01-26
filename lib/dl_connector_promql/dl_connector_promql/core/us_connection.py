from __future__ import annotations

from typing import (
    ClassVar,
    Optional,
)

import attr

from dl_core.services_registry import ServicesRegistry
from dl_core.us_connection_base import (
    ClassicConnectionSQL,
    ConnectionBase,
)
from dl_core.utils import secrepr
from dl_utils.utils import DataKey

from dl_connector_promql.core.constants import (
    SOURCE_TYPE_PROMQL,
    PromQLAuthType,
)
from dl_connector_promql.core.dto import PromQLConnDTO


class PromQLConnection(ClassicConnectionSQL):
    allow_cache: ClassVar[bool] = True
    is_always_user_source: ClassVar[bool] = True
    allow_dashsql: ClassVar[bool] = True
    source_type = SOURCE_TYPE_PROMQL

    @attr.s(kw_only=True)
    class DataModel(ClassicConnectionSQL.DataModel):
        path: Optional[str] = attr.ib(default=None)
        secure: bool = attr.ib(default=False)
        auth_type: PromQLAuthType = attr.ib()
        auth_header: str | None = attr.ib(repr=secrepr, default=None)

        @classmethod
        def get_secret_keys(cls) -> set[DataKey]:
            return {
                *super().get_secret_keys(),
                DataKey(parts=("auth_header",)),
            }

    def get_conn_dto(self) -> PromQLConnDTO:
        return PromQLConnDTO(
            conn_id=self.uuid,
            host=self.data.host,
            port=self.data.port,
            path=self.data.path,
            auth_type=self.data.auth_type,
            username=self.data.username,
            password=self.data.password,
            auth_header=self.data.auth_header,
            db_name=self.data.db_name or "",
            protocol="https" if self.data.secure else "http",
            multihosts=(),
        )

    @property
    def is_dashsql_allowed(self) -> bool:
        return True

    @property
    def is_subselect_allowed(self) -> bool:
        return False

    @property
    def allow_public_usage(self) -> bool:
        return True

    async def validate_new_data(
        self,
        services_registry: ServicesRegistry,
        changes: Optional[dict] = None,
        original_version: Optional[ConnectionBase] = None,
    ) -> None:
        if original_version is None or original_version and original_version.data.auth_type != self.data.auth_type:
            if self.data.auth_type == PromQLAuthType.header:
                self.data.username = None
                self.data.password = None
            elif self.data.auth_type == PromQLAuthType.password:
                self.data.auth_header = None
