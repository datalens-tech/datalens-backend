from __future__ import annotations

import attr

from bi_connector_postgresql.core.postgresql_base.dto import PostgresConnDTOBase
from bi_connector_greenplum.core.constants import CONNECTION_TYPE_GREENPLUM


@attr.s(frozen=True)
class GreenplumConnDTO(PostgresConnDTOBase):
    conn_type = CONNECTION_TYPE_GREENPLUM
