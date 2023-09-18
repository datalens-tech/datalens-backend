from __future__ import annotations

import attr

from dl_connector_bigquery.core.constants import CONNECTION_TYPE_BIGQUERY
from dl_core.connection_models.dto_defs import ConnDTO
from dl_core.utils import secrepr


@attr.s(frozen=True)
class BigQueryConnDTO(ConnDTO):
    conn_type = CONNECTION_TYPE_BIGQUERY

    credentials: str = attr.ib(repr=secrepr)  # corresponds to `credentials_base64` in BQ SQLA
    project_id: str = attr.ib()
