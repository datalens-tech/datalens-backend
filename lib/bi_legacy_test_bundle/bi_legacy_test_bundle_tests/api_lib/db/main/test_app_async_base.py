from __future__ import annotations

import pytest
import shortuuid

from dl_configs.enums import RequiredService


@pytest.mark.asyncio
async def test_ping(async_api_local_env_low_level_client):
    client = async_api_local_env_low_level_client
    req_id = shortuuid.uuid()

    resp = await client.get("/ping", headers={'x-request-id': req_id})
    assert resp.status == 200
    js = await resp.json()
    assert js['request_id'].startswith(req_id + '--')


@pytest.mark.asyncio
async def test_ping_ready(async_api_local_env_low_level_client):
    client = async_api_local_env_low_level_client
    req_id = shortuuid.uuid()

    resp = await client.get("/ping_ready", headers={'x-request-id': req_id})
    assert resp.status == 200
    js = await resp.json()
    assert js['request_id'].startswith(req_id + '--')
    details = js['details']
    assert details[RequiredService.POSTGRES.name] is True
    assert details[RequiredService.RQE_INT_SYNC.name] == 200
    assert details[RequiredService.RQE_EXT_SYNC.name] == 200
