from __future__ import annotations

from aiohttp import web
import pytest

from dl_core.base_models import PathEntryLocation
from dl_core.exc import USBadRequestException
from dl_core.united_storage_client import USAuthContextMaster
from dl_core.united_storage_client_aio import UStorageClientAIO


@pytest.mark.asyncio
async def test_fields_masking(aiohttp_client, caplog):
    caplog.set_level("INFO", logger="dl_core.united_storage_client")

    @web.middleware
    async def always_400_mw(request, handler):
        return web.json_response({}, status=400)

    app = web.Application(middlewares=[always_400_mw])

    mock = await aiohttp_client(app)

    us_client = UStorageClientAIO(
        auth_ctx=USAuthContextMaster("fake_token"), host=f"http://{mock.host}:{mock.port}", prefix="/api/private"
    )

    secret_value = "some_cypher_text_898"
    non_secret_value = "some_non_secret_value_989"

    with pytest.raises(USBadRequestException):
        await us_client.create_entry(
            key=PathEntryLocation("fake_key"),
            scope="connection",
            data=dict(
                password=secret_value,
                plain_val=non_secret_value,
            ),
            unversioned_data=dict(token=dict(cypher_text=secret_value)),
        )

    try:
        rec = next(r for r in caplog.records if r.message.startswith("US error response on"))
    # StopIteration causes RuntimeError in async test launcher
    except StopIteration as err:
        raise AssertionError() from err

    assert non_secret_value in rec.message, f"Expected non-secret {repr(secret_value)} not found in logs: {repr(rec)}"
    assert secret_value not in rec.message, f"Secret {repr(secret_value)} found in logs: {repr(rec)}"
