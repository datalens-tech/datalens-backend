from __future__ import annotations

from datetime import datetime
from typing import Optional

import attr

from dl_connector_snowflake.core.constants import CONNECTION_TYPE_SNOWFLAKE
from dl_core.connection_models.dto_defs import ConnDTO
from dl_core.utils import secrepr


@attr.s(frozen=True)
class SnowFlakeConnDTO(ConnDTO):
    conn_type = CONNECTION_TYPE_SNOWFLAKE

    account_name: str = attr.ib()
    user_name: str = attr.ib()
    user_role: Optional[str] = attr.ib()
    client_id: str = attr.ib()
    client_secret: str = attr.ib(repr=secrepr)

    refresh_token: str = attr.ib(repr=secrepr)
    refresh_token_expire_time: Optional[datetime] = attr.ib()

    schema: str = attr.ib()
    db_name: str = attr.ib()
    warehouse: str = attr.ib()
