from __future__ import annotations

import attr

from dl_connector_postgresql.core.postgresql.constants import CONNECTION_TYPE_POSTGRES
from dl_connector_postgresql.core.postgresql_base.dto import PostgresConnDTOBase


@attr.s(frozen=True)
class PostgresConnDTO(PostgresConnDTOBase):
    conn_type = CONNECTION_TYPE_POSTGRES
