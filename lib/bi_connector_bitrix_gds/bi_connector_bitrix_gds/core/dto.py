from __future__ import annotations

import attr

from bi_core.connection_models.dto_defs import ConnDTO
from bi_core.utils import secrepr

from bi_connector_bitrix_gds.core.constants import CONNECTION_TYPE_BITRIX24


@attr.s(frozen=True)
class BitrixGDSConnDTO(ConnDTO):
    conn_type = CONNECTION_TYPE_BITRIX24

    portal: str = attr.ib(kw_only=True)
    token: str = attr.ib(kw_only=True, repr=secrepr)
