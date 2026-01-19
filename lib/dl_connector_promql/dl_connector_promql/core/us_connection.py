from __future__ import annotations

from typing import (
    ClassVar,
    Optional,
)

import attr

from dl_core.us_connection_base import ClassicConnectionSQL
from dl_core.utils import secrepr

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

    def get_conn_dto(self) -> PromQLConnDTO:
        return PromQLConnDTO(
            conn_id=self.uuid,
            host=self.data.host,
            port=self.data.port,
            path=self.data.path,
            auth_type=self.data.auth_type,
            username=self.data.username or "",
            password=self.data.password or "",
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
