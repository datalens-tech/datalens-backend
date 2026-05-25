import attrs

import dl_httpx


@attrs.define(kw_only=True, frozen=True)
class PingRequest(dl_httpx.BaseRequest):
    # auth_provider is not needed for ping request
    @property
    def path(self) -> str:
        return "/ping"

    @property
    def method(self) -> str:
        return "GET"
