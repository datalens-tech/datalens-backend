from __future__ import annotations

from .exceptions import NotFound
from .utils import json
from .db_utils import db_get_one
from .manager_aiopg_base import DLSPGDB, ensure_db_reading, ensure_db_writing


class DLSPGConfs(DLSPGDB):
    """ Configurations supporting code """

    @ensure_db_reading
    async def get_configuration_async(self, name):
        Data = self.Data
        key = 'configuration__{}'.format(name)
        rows = await self.db_conn.execute(Data.select_(Data.key == key).limit(2))
        try:
            value = await db_get_one(rows)
        except NotFound:
            return {}
        return json.loads(value)

    @ensure_db_writing
    async def set_configuration_async(self, name, data):
        Data = self.Data
        key = 'configuration__{}'.format(name)
        data_s = json.dumps(data)
        return await self.db_conn.execute(Data.insert_().values(key=key, data=data_s))

    @ensure_db_reading
    async def _get_scopes_info_async(self):
        if not self.custom_scopes_enabled:
            return {}
        return await self.get_configuration_async('scopes') or {}

    @ensure_db_reading
    async def _get_scope_info_async(self, scope, scopes_info=None):
        if not self.custom_scopes_enabled:
            return self._get_scope_info(scope, scopes_info=scopes_info)

        if scopes_info is None:
            scopes_info = await self._get_scopes_info_async()
        scope_conf = scopes_info.get(scope)
        if scope_conf is None:
            raise ValueError("Unknown scope", scope)
        return self._normalize_scope_info(scope_conf)
