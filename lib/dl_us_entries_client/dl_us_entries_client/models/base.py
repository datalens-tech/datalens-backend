import logging

import attrs

import dl_httpx
from dl_us_entries_client.protocols.tenant import TenantProtocol


LOGGER = logging.getLogger(__name__)


@attrs.define(kw_only=True, frozen=True)
class BaseRequest(dl_httpx.BaseRequest):
    tenant: TenantProtocol | None = None

    @property
    def headers(self) -> dict[str, str]:
        result = super().headers
        if self.tenant is not None:
            tenant_headers = self.tenant.get_outbound_tenancy_headers()
            result.update({k.value.lower(): v for k, v in tenant_headers.items()})
        return result
