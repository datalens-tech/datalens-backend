import aiohttp
import pytest


TEST_USER_IP = "123.45.67.89"


@pytest.mark.asyncio
async def test_user_ip_from_real_ip_header(app_client: aiohttp.ClientSession) -> None:
    response = await app_client.get(
        "/api/v1/headers",
        headers={"X-Real-Ip": TEST_USER_IP},
    )
    assert response.status == 200
    response_json = await response.json()

    assert response_json["user_ip"] == TEST_USER_IP


@pytest.mark.asyncio
async def test_user_ip_from_forwarded_for_header(app_client: aiohttp.ClientSession) -> None:
    response = await app_client.get(
        "/api/v1/headers",
        headers={"X-Forwarded-For": f"1.2.3.4,{TEST_USER_IP},9.10.11.12"},
    )
    assert response.status == 200
    response_json = await response.json()

    assert response_json["user_ip"] == TEST_USER_IP


@pytest.mark.asyncio
async def test_user_ip_from_remote(app_client: aiohttp.ClientSession) -> None:
    response = await app_client.get(
        "/api/v1/headers",
    )
    assert response.status == 200
    response_json = await response.json()

    assert response_json["user_ip"] == "127.0.0.1"
