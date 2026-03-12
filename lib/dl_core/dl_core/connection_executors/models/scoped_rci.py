import attr
from typing_extensions import Self

import dl_api_commons
import dl_auth
from dl_obfuscator.engine import ObfuscationEngine


@attr.s(frozen=True)
class DBAdapterScopedRCI:
    request_id: str | None = attr.ib(default=None)
    x_dl_debug_mode: bool | None = attr.ib(default=None)
    user_name: str | None = attr.ib(default=None)
    client_ip: str | None = attr.ib(default=None, repr=False)
    auth_data: dl_auth.AuthData | None = attr.ib(default=None, repr=False)
    obfuscation_engine: ObfuscationEngine | None = attr.ib(default=None, repr=False)

    @classmethod
    def from_full_rci(cls, full_rci: dl_api_commons.RequestContextInfo) -> Self:
        return cls(
            request_id=full_rci.request_id,
            x_dl_debug_mode=full_rci.x_dl_debug_mode,
            user_name=full_rci.user_name,
            client_ip=full_rci.client_ip,
            auth_data=full_rci.auth_data,
            obfuscation_engine=full_rci.obfuscation_engine,
        )
