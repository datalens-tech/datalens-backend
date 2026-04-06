import aiohttp
import pytest

import dl_app_api_ext_testing.base as base


class DocsExtTestSuite(base.ExtTestSuiteBase):
    @pytest.mark.asyncio
    async def test_docs_spec(
        self,
        app_client: aiohttp.ClientSession,
    ) -> None:
        response = await app_client.get("api/v1/docs/spec.json")
        assert response.status == 200

    @pytest.mark.asyncio
    async def test_docs_spec_unauthorized(
        self,
        unauthorized_app_client: aiohttp.ClientSession,
    ) -> None:
        response = await unauthorized_app_client.get("api/v1/docs/spec.json")
        assert response.status == 401

    @pytest.mark.asyncio
    async def test_docs_swagger_ui(
        self,
        app_client: aiohttp.ClientSession,
    ) -> None:
        response = await app_client.get("api/v1/docs")
        assert response.status == 200
        assert "/api/v1/docs/static/swagger-ui.css" in await response.text()

    @pytest.mark.asyncio
    async def test_docs_swagger_ui_unauthorized(
        self,
        unauthorized_app_client: aiohttp.ClientSession,
    ) -> None:
        response = await unauthorized_app_client.get("api/v1/docs")
        assert response.status == 401
