from __future__ import annotations

import pytest
import shortuuid

from dl_api_lib_testing.api_base import DefaultApiTestBase
from dl_configs.enums import RequiredService


class TestPing(DefaultApiTestBase):
    @pytest.mark.asyncio
    async def test_ping(self, data_api_lowlevel_aiohttp_client):
        client = data_api_lowlevel_aiohttp_client
        req_id = shortuuid.uuid()

        resp = await client.get("/ping", headers={"x-request-id": req_id})
        assert resp.status == 200
        js = await resp.json()
        assert js["request_id"].startswith(req_id + "--")

    @pytest.mark.asyncio
    async def test_ping_ready(self, data_api_lowlevel_aiohttp_client):
        client = data_api_lowlevel_aiohttp_client
        req_id = shortuuid.uuid()

        resp = await client.get("/ping_ready", headers={"x-request-id": req_id})
        assert resp.status == 200
        js = await resp.json()
        assert js["request_id"].startswith(req_id + "--")
        details = js["details"]
        assert details[RequiredService.POSTGRES.name] is True
        assert details[RequiredService.RQE_INT_SYNC.name] == 200
        assert details[RequiredService.RQE_EXT_SYNC.name] == 200
