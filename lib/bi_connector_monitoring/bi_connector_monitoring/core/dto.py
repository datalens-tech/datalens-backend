from __future__ import annotations

import attr

from bi_constants.enums import ConnectionType

from bi_core.connection_models.dto_defs import ConnDTO


@attr.s(frozen=True)
class MonitoringConnDTO(ConnDTO):
    conn_type = ConnectionType.monitoring

    host: str = attr.ib(kw_only=True)
    url_path: str = attr.ib(kw_only=True)
    service_account_id: str = attr.ib(kw_only=True)
    folder_id: str = attr.ib(kw_only=True)
