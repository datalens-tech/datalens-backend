from __future__ import annotations

import attr

from dl_core.connection_models.conn_options import ConnectOptions


@attr.s(frozen=True, hash=True)
class CHConnectOptions(ConnectOptions):
    max_execution_time: int | None = attr.ib(default=None)
    connect_timeout: int | None = attr.ib(default=None)
    total_timeout: int | None = attr.ib(default=None)
    disable_value_processing: bool = attr.ib(default=False)

    # TODO remove in the next release to avoid compatibility issues
    insert_quorum: int | None = attr.ib(default=None)
    insert_quorum_timeout: int | None = attr.ib(default=None)
