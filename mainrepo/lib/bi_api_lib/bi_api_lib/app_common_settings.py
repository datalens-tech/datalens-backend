from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Callable,
    List,
    Optional,
)

if TYPE_CHECKING:
    from bi_core.connection_models import ConnectOptions
    from bi_core.us_connection_base import ExecutorBasedMixin


_T_CONN_OPTIONS_MUTATOR = Callable[["ConnectOptions", "ExecutorBasedMixin"], Optional["ConnectOptions"]]


class ConnOptionsMutatorsFactory:
    def __init__(self) -> None:
        self.mutators: List[_T_CONN_OPTIONS_MUTATOR] = []

    def add_mutator(self, func: _T_CONN_OPTIONS_MUTATOR) -> None:
        self.mutators.append(func)

    def __call__(self, conn: ExecutorBasedMixin) -> Optional[ConnectOptions]:
        any_mutated = False
        conn_opts = conn.get_conn_options()
        for func in self.mutators:
            mutated = func(conn_opts, conn)
            if mutated is not None:
                any_mutated = True
                conn_opts = mutated
        # TODO?: is there any sense in returning `None`, though?
        return conn_opts if any_mutated else None
