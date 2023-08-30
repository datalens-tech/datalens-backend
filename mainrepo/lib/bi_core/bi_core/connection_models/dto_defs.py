from __future__ import annotations

from typing import ClassVar, Optional, List

import attr

from bi_constants.enums import ConnectionType


@attr.s(frozen=True)
class ConnDTO:
    conn_type: ClassVar[ConnectionType]

    conn_id: Optional[str] = attr.ib(kw_only=True)

    def conn_reporting_data(self) -> dict:
        return dict(
            connection_id=self.conn_id,
        )


@attr.s(frozen=True)
class DefaultSQLDTO(ConnDTO):  # noqa
    host: str = attr.ib(kw_only=True)
    multihosts: tuple[str, ...] = attr.ib(kw_only=True, converter=tuple)
    port: int = attr.ib(kw_only=True)
    db_name: str = attr.ib(kw_only=True)
    username: str = attr.ib(kw_only=True)
    password: str = attr.ib(repr=False, kw_only=True)

    def get_all_hosts(self) -> List[str]:
        return (
            list(self.multihosts) if self.multihosts
            else [self.host] if self.host
            else []
        )

    def conn_reporting_data(self) -> dict:
        return super().conn_reporting_data() | dict(
            host=self.host,
        )
