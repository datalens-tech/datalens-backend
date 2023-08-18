from __future__ import annotations

import attr

from bi_constants.enums import ConnectionType

from bi_core.connection_models.dto_defs import DefaultSQLDTO


@attr.s(frozen=True)
class PromQLConnDTO(DefaultSQLDTO):
    conn_type = ConnectionType.promql

    protocol: str = attr.ib(kw_only=True)
