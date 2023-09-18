from __future__ import annotations

from typing import ClassVar

import attr

from dl_connector_promql.core.dto import PromQLConnDTO
from dl_core.us_connection_base import ClassicConnectionSQL


class PromQLConnection(ClassicConnectionSQL):
    allow_cache: ClassVar[bool] = True
    is_always_user_source: ClassVar[bool] = True
    allow_dashsql: ClassVar[bool] = True

    @attr.s(kw_only=True)
    class DataModel(ClassicConnectionSQL.DataModel):
        secure: bool = attr.ib(default=False)

    def get_conn_dto(self) -> PromQLConnDTO:
        return PromQLConnDTO(
            conn_id=self.uuid,
            host=self.data.host,
            port=self.data.port,
            username=self.data.username,
            password=self.data.password,
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
