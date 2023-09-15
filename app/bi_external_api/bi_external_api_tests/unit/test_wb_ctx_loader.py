import pytest
from aiohttp import web
from aiohttp.web_response import json_response

import bi_api_commons.exc
from bi_api_commons.base_models import TenantCommon, NoAuthData
from bi_external_api.internal_api_clients import exc_api
from bi_external_api.internal_api_clients.dataset_api import APIClientBIBackControlPlane
from bi_testing.utils import skip_outside_devhost


async def _cause_empty_404(request):
    return web.Response(status=404)


async def _cause_404_with_unexpected_body(request):
    return web.Response(status=404, text="something went wrong")


@skip_outside_devhost
@pytest.mark.parametrize("err_fn", [
    _cause_empty_404,
    _cause_404_with_unexpected_body,
])
@pytest.mark.asyncio
async def test_get_workbook_backend_toc_bad_404(aiohttp_client, intranet_user_1_creds, err_fn):
    app = web.Application()
    app.router.add_get("/api/v1/info/internal/pseudo_workbook/wb_id", err_fn)

    client = await aiohttp_client(app)

    api_control_plane = APIClientBIBackControlPlane(
        base_url=f"http://{client.server.host}:{client.server.port}",
        tenant=TenantCommon(),
        auth_data=NoAuthData(),
    )

    with pytest.raises(bi_api_commons.exc.NotFoundErr) as exc:
        await api_control_plane.get_workbook_backend_toc("wb_id")

    assert not isinstance(exc.value, exc_api.WorkbookNotFound)


async def _cause_proper_404(request):
    return json_response(data={
        "details": {},
        "code": "ERR.DS_API.US.OBJ_NOT_FOUND",
        "message": "Object not found", "debug": {}
    }, status=404)


@skip_outside_devhost
@pytest.mark.asyncio
async def test_get_workbook_backend_toc_nice_404(aiohttp_client, intranet_user_1_creds):
    app = web.Application()
    app.router.add_get("/api/v1/info/internal/pseudo_workbook/wb_id", _cause_proper_404)

    client = await aiohttp_client(app)

    api_control_plane = APIClientBIBackControlPlane(
        base_url=f"http://{client.server.host}:{client.server.port}",
        tenant=TenantCommon(),
        auth_data=NoAuthData(),
    )

    with pytest.raises(exc_api.WorkbookNotFound):
        await api_control_plane.get_workbook_backend_toc("wb_id")
