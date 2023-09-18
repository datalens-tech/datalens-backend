from __future__ import annotations

import attr

from dl_core.connection_models.dto_defs import ConnDTO

from bi_connector_monitoring.core.constants import CONNECTION_TYPE_MONITORING


@attr.s(frozen=True)
class MonitoringConnDTO(ConnDTO):
    conn_type = CONNECTION_TYPE_MONITORING

    host: str = attr.ib(kw_only=True)
    url_path: str = attr.ib(kw_only=True)
    service_account_id: str = attr.ib(kw_only=True)
    folder_id: str = attr.ib(kw_only=True)
