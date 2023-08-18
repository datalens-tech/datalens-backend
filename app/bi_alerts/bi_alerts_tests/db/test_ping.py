from __future__ import annotations

import pytest


@pytest.mark.asyncio
async def test_ping(web_app):
    resp = await web_app.get('/alerts/ping')
    resp_json = await resp.json()
    assert resp_json['request_id'].startswith('alert')


@pytest.mark.asyncio
async def test_ping_db(web_app):
    resp = await web_app.get('/alerts/ping_db')
    assert resp.status == 200
    resp_json = await resp.json()
    assert resp_json['db_status'] == 'ok'
