import asyncio

import aiohttp.test_utils as aiohttp_test_utils
import aiohttp.web as aiohttp_web
import flaky
import pytest
import pytest_aiohttp.plugin as pytest_aiohttp_plugin
import pytest_asyncio

import dl_rate_limiter


@pytest_asyncio.fixture(name="aiohttp_test_client", scope="function")
async def fixture_aiohttp_test_client(
    aiohttp_client: pytest_aiohttp_plugin.AiohttpClient,
    async_request_limiter: dl_rate_limiter.AsyncRequestRateLimiter,
) -> aiohttp_test_utils.TestClient:
    middleware = dl_rate_limiter.AioHTTPMiddleware(rate_limiter=async_request_limiter)

    app = aiohttp_web.Application(middlewares=[middleware.process])

    async def handler(request: aiohttp_web.Request):
        raw_data = await request.read()
        return aiohttp_web.Response(body=raw_data, status=200)

    app.router.add_route("GET", "/{tail:.*}", handler)

    return await aiohttp_client(app)


@pytest.mark.asyncio
async def test_hello(aiohttp_test_client: aiohttp_test_utils.TestClient):
    response = await aiohttp_test_client.get("/hello", data="OK")
    data = await response.read()
    assert data == b"OK"


@pytest.mark.asyncio
async def test_unlimited(aiohttp_test_client: aiohttp_test_utils.TestClient):
    tasks = [aiohttp_test_client.get("/unlimited/1", headers={"X-Test-Header": "test"}) for _ in range(10)]
    responses = await asyncio.gather(*tasks)

    assert all(response.status == 200 for response in responses)


@pytest.mark.asyncio
async def test_limited(aiohttp_test_client: aiohttp_test_utils.TestClient):
    tasks = [aiohttp_test_client.get("/limited/1", headers={"X-Test-Header": "test"}) for _ in range(20)]
    responses = await asyncio.gather(*tasks)

    assert sum(response.status == 429 for response in responses) == 15
    assert sum(response.status == 200 for response in responses) == 5


@flaky.flaky(max_runs=3)
@pytest.mark.asyncio
async def test_limited_unique_headers(aiohttp_test_client: aiohttp_test_utils.TestClient):
    tasks = [aiohttp_test_client.get("/limited/1", headers={"X-Test-Header": str(i)}) for i in range(10)]
    responses = await asyncio.gather(*tasks)

    assert all(response.status == 200 for response in responses)


@flaky.flaky(max_runs=3)
@pytest.mark.asyncio
async def test_limited_multiple_limits(aiohttp_test_client: aiohttp_test_utils.TestClient):
    tasks = [
        aiohttp_test_client.get("/limited/more_specifically/1", headers={"X-Test-Header": "test"}) for _ in range(10)
    ]
    responses = await asyncio.gather(*tasks)

    assert sum(response.status == 429 for response in responses) == 9
    assert sum(response.status == 200 for response in responses) == 1

    # checking that less specific limit is also affected
    tasks = [aiohttp_test_client.get("/limited/1", headers={"X-Test-Header": "test"}) for _ in range(20)]
    responses = await asyncio.gather(*tasks)

    assert sum(response.status == 429 for response in responses) == 16
    assert sum(response.status == 200 for response in responses) == 4


@flaky.flaky(max_runs=3)
@pytest.mark.asyncio
async def test_regex_limited(aiohttp_test_client: aiohttp_test_utils.TestClient):
    tasks = [aiohttp_test_client.get("/regex/1", headers={"X-Test-Header": "test"}) for _ in range(20)]
    responses = await asyncio.gather(*tasks)

    assert sum(response.status == 429 for response in responses) == 15
    assert sum(response.status == 200 for response in responses) == 5


@flaky.flaky(max_runs=3)
@pytest.mark.asyncio
async def test_regex_limited_unique_headers(aiohttp_test_client: aiohttp_test_utils.TestClient):
    tasks = [aiohttp_test_client.get("/regex/1", headers={"X-Test-Header": "1"}) for i in range(20)]
    responses = await asyncio.gather(*tasks)

    assert all(response.status == 200 for response in responses)
