from __future__ import annotations

from typing import Optional

import attr

from bi_connector_chyt.core.conn_options import CHYTConnectOptions


@attr.s(frozen=True, hash=True)
class CHYTInternalConnectOptions(CHYTConnectOptions):
    mirroring_frac: float = attr.ib(default=0.0)
    mirroring_clique_alias: Optional[str] = attr.ib(default=None)
    mirroring_clique_req_timeout_sec: Optional[float] = attr.ib(default=None)
