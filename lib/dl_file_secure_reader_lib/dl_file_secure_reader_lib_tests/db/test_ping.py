from __future__ import annotations

import pytest


@pytest.mark.asyncio
async def test_ping(web_app):
    resp = await web_app.get("/reader/ping")
    assert resp.status == 200
