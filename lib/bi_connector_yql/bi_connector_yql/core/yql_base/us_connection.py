from __future__ import annotations

from typing import Optional

from bi_connector_yql.core.yql_base.utils import validate_service_account_id
from dl_core.us_connection_base import ConnectionBase


class YQLConnectionMixin(ConnectionBase):
    """ Common YDB/YQ USConnection logic mixin  """

    async def validate_new_data(
            self,
            changes: Optional[dict] = None,
            original_version: Optional[ConnectionBase] = None,
    ) -> None:
        await super().validate_new_data(changes=changes, original_version=original_version)
        svcacc_id = self.data.service_account_id
        if not svcacc_id:
            return  # no svcacc id, nothing to check
        if original_version is not None:
            original_svcacc_id = getattr(original_version.data, 'service_account_id')
            if svcacc_id == original_svcacc_id:
                return  # no change, allowing continued use
        context = self._context
        if not context:
            raise Exception(f"f{self.__class__.__name__}._context is required")
        services_registry = self.us_manager.get_services_registry()
        if services_registry is None:
            raise Exception(f"{self.__class__.__name__}.us_manager.get_services_registry() returned None")
        await validate_service_account_id(svcacc_id, context=context, services_registry=services_registry)
