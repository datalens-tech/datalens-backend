from __future__ import annotations

from typing import Optional

import attr

from dl_core.connection_executors.models.connection_target_dto_base import ConnTargetDTO
from dl_core.utils import secrepr

from dl_connector_snowflake.core.dto import SnowFlakeConnDTO


@attr.s(frozen=True, kw_only=True)
class SnowFlakeConnTargetDTO(ConnTargetDTO):
    account_name: str = attr.ib()
    user_name: str = attr.ib()
    user_role: Optional[str] = attr.ib()

    schema: str = attr.ib()
    db_name: str = attr.ib()
    warehouse: str = attr.ib()

    access_token: str = attr.ib(repr=secrepr)

    @classmethod
    def from_dto(cls, dto: SnowFlakeConnDTO, access_token: str) -> SnowFlakeConnTargetDTO:
        return cls(
            access_token=access_token,
            account_name=dto.account_name,
            user_name=dto.user_name,
            user_role=dto.user_role,
            schema=dto.schema,
            db_name=dto.db_name,
            warehouse=dto.warehouse,
            conn_id=dto.conn_id,
        )

    def get_effective_host(self) -> Optional[str]:
        return None
