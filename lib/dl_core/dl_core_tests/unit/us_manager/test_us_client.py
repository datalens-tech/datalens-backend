from aiohttp import web
import pytest
import responses

from dl_core.base_models import PathEntryLocation
from dl_core.exc import (
    USBadRequestError,
    USReqError,
)
from dl_core.united_storage_client import (
    USAuthContextMaster,
    UStorageClient,
)
from dl_core.united_storage_client_aio import UStorageClientAIO
import dl_retrier


@pytest.mark.asyncio
async def test_fields_masking(aiohttp_client, caplog, root_certificates):
    caplog.set_level("INFO", logger="dl_core.united_storage_client")

    @web.middleware
    async def always_400_mw(request, handler):
        return web.json_response({}, status=400)

    app = web.Application(middlewares=[always_400_mw])

    mock = await aiohttp_client(app)

    us_client = UStorageClientAIO(
        auth_ctx=USAuthContextMaster(us_master_token="fake_token"),
        host=f"http://{mock.host}:{mock.port}",
        prefix="/api/private",
        ca_data=root_certificates,
        retry_policy_factory=dl_retrier.DefaultRetryPolicyFactory(),
    )

    secret_value = "some_cypher_text_898"
    non_secret_value = "some_non_secret_value_989"

    with pytest.raises(USBadRequestError):
        await us_client.create_entry(
            key=PathEntryLocation("fake_key"),
            scope="connection",
            data={
                "password": secret_value,
                "plain_val": non_secret_value,
            },
            unversioned_data={"token": {"cypher_text": secret_value}},
        )

    try:
        rec = next(r for r in caplog.records if r.message.startswith("US error response on"))
    # StopIteration causes RuntimeError in async test launcher
    except StopIteration as err:
        raise AssertionError() from err

    assert non_secret_value in rec.message, f"Expected non-secret {secret_value!r} not found in logs: {rec!r}"
    assert secret_value not in rec.message, f"Secret {secret_value!r} found in logs: {rec!r}"


@responses.activate
def test_us_sync_client_retry_count_matches_policy() -> None:
    responses.add(responses.GET, "http://us.example.com/private/entries/dummy", json={}, status=500)

    factory = dl_retrier.RetryPolicyFactory.from_settings(
        dl_retrier.RetryPolicyFactorySettings(
            DEFAULT_POLICY=dl_retrier.RetryPolicySettings(
                RETRIES_COUNT=2,
                BACKOFF_INITIAL=0,
                BACKOFF_FACTOR=0,
                BACKOFF_MAX=0,
            )
        )
    )
    client = UStorageClient(
        host="http://us.example.com",
        auth_ctx=USAuthContextMaster(us_master_token="fake_token"),
        retry_policy_factory=factory,
    )
    try:
        with pytest.raises(USReqError):
            client._request(
                UStorageClient.RequestData(
                    method="get",
                    relative_url="/entries/dummy",
                    params=None,
                    json=None,
                )
            )
    finally:
        client.close()

    assert len(responses.calls) == 3
