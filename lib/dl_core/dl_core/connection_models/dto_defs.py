from __future__ import annotations

from typing import (
    Any,
    ClassVar,
    Self,
)

import attr

from dl_constants import ConnectionType


@attr.s(frozen=True)
class ConnDTO:
    conn_type: ClassVar[ConnectionType]

    conn_id: str | None = attr.ib(kw_only=True)

    def conn_reporting_data(self) -> dict:
        return {
            "connection_id": self.conn_id,
        }

    def clone(self, **kwargs: Any) -> Self:
        return attr.evolve(self, **kwargs)


def to_tuple(v: Any) -> tuple:
    return tuple(v)


@attr.s(frozen=True)
class DefaultSQLDTO(ConnDTO):
    host: str = attr.ib(kw_only=True)
    multihosts: tuple[str, ...] = attr.ib(kw_only=True, converter=to_tuple)
    port: int = attr.ib(kw_only=True)
    db_name: str = attr.ib(kw_only=True)
    username: str = attr.ib(kw_only=True)
    password: str = attr.ib(repr=False, kw_only=True)

    def get_all_hosts(self) -> list[str]:
        return list(self.multihosts) if self.multihosts else [self.host] if self.host else []

    def conn_reporting_data(self) -> dict:
        return super().conn_reporting_data() | {
            "host": self.host,
        }
