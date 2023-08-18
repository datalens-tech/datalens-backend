from __future__ import annotations

from typing import Optional

import attr

from bi_core.connection_models.conn_options import ConnectOptions


@attr.s(frozen=True, hash=True)
class CHConnectOptions(ConnectOptions):
    max_execution_time: Optional[int] = attr.ib(default=None)
    connect_timeout: Optional[int] = attr.ib(default=None)
    total_timeout: Optional[int] = attr.ib(default=None)
    insert_quorum: Optional[int] = attr.ib(default=None)
    insert_quorum_timeout: Optional[int] = attr.ib(default=None)
    disable_value_processing: bool = attr.ib(default=False)
