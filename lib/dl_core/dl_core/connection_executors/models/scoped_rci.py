from __future__ import annotations

from typing import Optional

import attr

from dl_api_commons.base_models import RequestContextInfo


@attr.s(frozen=True)
class DBAdapterScopedRCI:
    request_id: Optional[str] = attr.ib(default=None)
    x_dl_debug_mode: Optional[bool] = attr.ib(default=None)
    user_name: Optional[str] = attr.ib(default=None)
    client_ip: Optional[str] = attr.ib(default=None, repr=False)

    @classmethod
    def from_full_rci(cls, full_rci: RequestContextInfo) -> DBAdapterScopedRCI:
        return cls(
            request_id=full_rci.request_id,
            x_dl_debug_mode=full_rci.x_dl_debug_mode,
            user_name=full_rci.user_name,
            client_ip=full_rci.client_ip,
        )

    @classmethod
    def create_empty(cls):
        return cls.from_full_rci(RequestContextInfo.create_empty())
