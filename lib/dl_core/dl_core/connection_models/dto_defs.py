from __future__ import annotations

from typing import (
    Any,
    ClassVar,
    Optional,
)

import attr
from typing_extensions import Self

from dl_constants.enums import ConnectionType


@attr.s(frozen=True)
class ConnDTO:
    conn_type: ClassVar[ConnectionType]

    conn_id: Optional[str] = attr.ib(kw_only=True)

    def conn_reporting_data(self) -> dict:
        return dict(
            connection_id=self.conn_id,
        )

    def clone(self, **kwargs: Any) -> Self:
        return attr.evolve(self, **kwargs)


@attr.s(frozen=True)
class DefaultSQLDTO(ConnDTO):  # noqa
    host: str = attr.ib(kw_only=True)
    multihosts: tuple[str, ...] = attr.ib(kw_only=True, converter=lambda x: tuple(x))
    port: int = attr.ib(kw_only=True)
    db_name: str = attr.ib(kw_only=True)
    username: str = attr.ib(kw_only=True)
    password: str = attr.ib(repr=False, kw_only=True)

    def get_all_hosts(self) -> list[str]:
        return list(self.multihosts) if self.multihosts else [self.host] if self.host else []

    def conn_reporting_data(self) -> dict:
        return super().conn_reporting_data() | dict(
            host=self.host,
        )
