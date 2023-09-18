from __future__ import annotations

from typing import (
    Any,
    Optional,
    Type,
    TypeVar,
)

import attr

_CONNECT_OPTIONS_TV = TypeVar("_CONNECT_OPTIONS_TV", bound="ConnectOptions")


@attr.s(frozen=True, hash=True)
class ConnectOptions:
    rqe_total_timeout: Optional[int] = attr.ib(default=None)
    rqe_sock_read_timeout: Optional[int] = attr.ib(default=None)
    use_managed_network: bool = attr.ib(default=True)    # TODO: temporary - remove in favor of MDBConnectOptionsMixin
    fetch_table_indexes: bool = attr.ib(default=False)
    pass_db_messages_to_user: bool = attr.ib(default=False)
    pass_db_query_to_user: bool = attr.ib(default=False)

    def to_subclass(self, subcls: Type[_CONNECT_OPTIONS_TV], **kwargs: Any) -> _CONNECT_OPTIONS_TV:
        assert issubclass(subcls, type(self))
        full_kwargs = {
            **attr.asdict(self),
            **kwargs,
        }
        # `error: Incompatible return value type (got "ConnectOptions", expected "_CONNECT_OPTIONS_TV")`
        return subcls(**full_kwargs)  # type: ignore  # TODO: fix

    def clone(self: _CONNECT_OPTIONS_TV, **kwargs: Any) -> _CONNECT_OPTIONS_TV:
        return attr.evolve(self, **kwargs)
