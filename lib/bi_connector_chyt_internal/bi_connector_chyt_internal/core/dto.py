from typing import Optional

import attr

from bi_connector_chyt.core.dto import BaseCHYTDTO

from bi_connector_chyt_internal.core.constants import (
    CONNECTION_TYPE_CH_OVER_YT,
    CONNECTION_TYPE_CH_OVER_YT_USER_AUTH,
)


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
    conn_type = CONNECTION_TYPE_CH_OVER_YT

    token: str = attr.ib(repr=False, kw_only=True)


@attr.s(frozen=True)
class CHYTUserAuthDTO(CHYTInternalBaseDTO):
    conn_type = CONNECTION_TYPE_CH_OVER_YT_USER_AUTH

    header_authorization: Optional[str] = attr.ib(repr=False, kw_only=True)
    header_cookie: Optional[str] = attr.ib(repr=False, kw_only=True)
