from __future__ import annotations

import attr

from dl_connector_greenplum.core.constants import CONNECTION_TYPE_GREENPLUM
from dl_connector_postgresql.core.postgresql_base.dto import PostgresConnDTOBase


@attr.s(frozen=True)
class GreenplumConnDTO(PostgresConnDTOBase):
    conn_type = CONNECTION_TYPE_GREENPLUM
