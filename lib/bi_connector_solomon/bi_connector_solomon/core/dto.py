from __future__ import annotations

from typing import Optional

import attr

from bi_core.connection_models.dto_defs import ConnDTO

from bi_connector_solomon.core.constants import CONNECTION_TYPE_SOLOMON


@attr.s(frozen=True)
class SolomonConnDTO(ConnDTO):
    conn_type = CONNECTION_TYPE_SOLOMON

    host: str = attr.ib(kw_only=True)
    cookie_session_id: Optional[str] = attr.ib(default=None, repr=False, kw_only=True)
    cookie_sessionid2: Optional[str] = attr.ib(default=None, repr=False, kw_only=True)
    user_ip: Optional[str] = attr.ib(default=None, kw_only=True)
    user_host: Optional[str] = attr.ib(default=None, kw_only=True)
