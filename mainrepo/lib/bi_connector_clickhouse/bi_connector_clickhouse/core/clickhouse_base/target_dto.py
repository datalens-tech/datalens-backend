from typing import Optional

import attr

from bi_core.connection_executors.models.connection_target_dto_base import BaseSQLConnTargetDTO


@attr.s(frozen=True)
class BaseClickHouseConnTargetDTO(BaseSQLConnTargetDTO):
    protocol: str = attr.ib()
    # TODO CONSIDER: Is really optional?
    endpoint: Optional[str] = attr.ib()
    cluster_name: Optional[str] = attr.ib()
    max_execution_time: Optional[int] = attr.ib()
    connect_timeout: Optional[int] = attr.ib()
    total_timeout: Optional[int] = attr.ib()
    insert_quorum: Optional[int] = attr.ib()
    insert_quorum_timeout: Optional[int] = attr.ib()
    disable_value_processing: bool = attr.ib()
    secure: bool = attr.ib(kw_only=True, default=False)
    ssl_ca: Optional[str] = attr.ib(kw_only=True, default=None)


class ClickHouseConnTargetDTO(BaseClickHouseConnTargetDTO):
    """..."""
