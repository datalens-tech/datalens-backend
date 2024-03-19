from __future__ import annotations

import attr

from dl_configs.rqe import RQEExecuteRequestMode


@attr.s(frozen=True)
class RemoteQueryExecutorData:
    hmac_key: bytes = attr.ib(repr=False)
    # async QE data
    async_protocol: str = attr.ib()
    async_host: str = attr.ib()
    async_port: int = attr.ib()
    # sync QE data
    sync_protocol: str = attr.ib()
    sync_host: str = attr.ib()
    sync_port: int = attr.ib()

    execute_request_mode: RQEExecuteRequestMode = attr.ib(default=RQEExecuteRequestMode.STREAM)
