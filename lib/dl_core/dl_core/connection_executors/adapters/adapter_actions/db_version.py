from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Optional,
)

import attr

from dl_core.connection_executors.adapters.adapter_actions.base import AsyncDBVersionAdapterAction
from dl_core.connection_executors.adapters.sa_utils import get_db_version_query
from dl_core.connection_models import DBIdent


if TYPE_CHECKING:
    from dl_core.connection_executors.adapters.async_adapters_base import AsyncDBAdapter


@attr.s(frozen=True)
class AsyncDBVersionAdapterActionNone(AsyncDBVersionAdapterAction):
    async def run_db_version_action(self, db_ident: DBIdent) -> Optional[str]:
        return None


@attr.s(frozen=True)
class AsyncDBVersionAdapterActionEmptyString(AsyncDBVersionAdapterAction):
    async def run_db_version_action(self, db_ident: DBIdent) -> Optional[str]:
        return ""


@attr.s(frozen=True)
class AsyncDBVersionAdapterActionViaFunctionQuery(AsyncDBVersionAdapterAction):
    _async_adapter: AsyncDBAdapter = attr.ib(kw_only=True)

    async def run_db_version_action(self, db_ident: DBIdent) -> Optional[str]:
        result = await self._async_adapter.execute(get_db_version_query(db_ident))
        async for row in result.get_all_rows():
            return str(row[0])
        return None
