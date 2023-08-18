from __future__ import annotations

import attr

from bi_constants.enums import ConnectionType

from bi_core.connection_models.dto_defs import ConnDTO
from bi_core.utils import secrepr


@attr.s(frozen=True)
class BitrixGDSConnDTO(ConnDTO):
    conn_type = ConnectionType.bitrix24

    portal: str = attr.ib(kw_only=True)
    token: str = attr.ib(kw_only=True, repr=secrepr)
