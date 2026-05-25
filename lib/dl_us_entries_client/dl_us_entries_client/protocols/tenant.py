from typing import Protocol

import dl_constants


class TenantProtocol(Protocol):
    def get_outbound_tenancy_headers(self) -> dict[dl_constants.DLHeaders, str]: ...
