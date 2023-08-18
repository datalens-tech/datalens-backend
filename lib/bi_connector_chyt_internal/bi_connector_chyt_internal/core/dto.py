from typing import Optional

import attr

from bi_constants.enums import ConnectionType
from bi_connector_chyt.core.dto import BaseCHYTDTO


@attr.s(frozen=True)
class CHYTInternalBaseDTO(BaseCHYTDTO):
    cluster: str = attr.ib(kw_only=True)

    def conn_reporting_data(self) -> dict:
        return super().conn_reporting_data() | dict(
            cluster=self.cluster,
            clique_alias=self.clique_alias,
        )


@attr.s(frozen=True)
class CHYTInternalDTO(CHYTInternalBaseDTO):
    conn_type = ConnectionType.ch_over_yt

    token: str = attr.ib(repr=False, kw_only=True)


@attr.s(frozen=True)
class CHYTUserAuthDTO(CHYTInternalBaseDTO):
    conn_type = ConnectionType.ch_over_yt_user_auth

    header_authorization: Optional[str] = attr.ib(repr=False, kw_only=True)
    header_cookie: Optional[str] = attr.ib(repr=False, kw_only=True)
