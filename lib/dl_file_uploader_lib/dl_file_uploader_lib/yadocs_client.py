from __future__ import annotations

from typing import Any

from aiohttp.client import ClientSession

from dl_file_uploader_lib import exc as file_upl_exc


def yadocs_error_to_file_uploader_exception(status_code: int, resp_info: dict) -> file_upl_exc.DLFileUploaderBaseError:
    if status_code == 401:
        err_cls = file_upl_exc.PermissionDenied
    elif status_code == 404:
        err_cls = file_upl_exc.DocumentNotFound
    elif status_code == 400:
        err_cls = file_upl_exc.UnsupportedDocument
    elif status_code >= 500:
        err_cls = file_upl_exc.RemoteServerError
    else:
        err_cls = file_upl_exc.DLFileUploaderBaseError

    return err_cls(
        details=resp_info,
    )


class YaDocsClient:
    headers: dict[str, Any] = {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    hostname: str = "https://cloud-api.yandex.net/v1/disk"

    def __init__(self, session: ClientSession):
        self.session = session

    async def get_spreadsheet_public_ref(self, link: str) -> str:
        resp = await self.session.get(
            f"{self.hostname}/public/resources/download/?public_key={link}",
            headers=self.headers,
        )
        if resp.status != 200:
            raise yadocs_error_to_file_uploader_exception(resp.status, await resp.json())
        return (await resp.json())["href"]

    async def get_spreadsheet_public_meta(self, link: str) -> dict[str, Any]:
        resp = await self.session.get(
            f"{self.hostname}/public/resources/?public_key={link}",
            headers=self.headers,
        )
        if resp.status != 200:
            raise yadocs_error_to_file_uploader_exception(resp.status, await resp.json())
        return await resp.json()

    async def get_spreadsheet_private_ref(self, path: str, token: str) -> str:
        headers_with_token = self._create_headers_with_token(token)
        resp = await self.session.get(
            f"{self.hostname}/resources/download/?path={path}",
            headers=headers_with_token,
        )
        if resp.status != 200:
            raise yadocs_error_to_file_uploader_exception(resp.status, await resp.json())
        return (await resp.json())["href"]

    async def get_spreadsheet_private_meta(self, path: str, token: str) -> dict[str, Any]:
        headers_with_token = self._create_headers_with_token(token)
        resp = await self.session.get(
            f"{self.hostname}/resources/?path={path}",
            headers=headers_with_token,
        )
        if resp.status != 200:
            raise yadocs_error_to_file_uploader_exception(resp.status, await resp.json())
        return await resp.json()

    def _create_headers_with_token(self, token: str) -> dict[str, Any]:
        headers_with_token = self.headers.copy()
        headers_with_token.update({"Authorization": "OAuth " + token})
        return headers_with_token
