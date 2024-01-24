from __future__ import annotations

from typing import (
    Any,
    ClassVar,
    Optional,
    TypeVar,
)

import attr

from dl_constants.enums import ConnectionType


_CONN_DTO_TV = TypeVar("_CONN_DTO_TV")


@attr.s(frozen=True)
class ConnDTO:
    conn_type: ClassVar[ConnectionType]

    conn_id: Optional[str] = attr.ib(kw_only=True)

    def conn_reporting_data(self) -> dict:
        return dict(
            connection_id=self.conn_id,
        )

    def clone(self: _CONN_DTO_TV, **kwargs: Any) -> _CONN_DTO_TV:
        return attr.evolve(self, **kwargs)  # type: ignore  # 2024-01-24 # TODO: Argument 1 to "evolve" has a variable type "_CONN_DTO_TV" not bound to an attrs class  [misc]


@attr.s(frozen=True)
class DefaultSQLDTO(ConnDTO):  # noqa
    host: str = attr.ib(kw_only=True)
    multihosts: tuple[str, ...] = attr.ib(kw_only=True, converter=tuple)
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
