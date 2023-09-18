import asyncio
import time

import aiohttp

import bi_integration_tests.request_executors.base as request_executors_base
from bi_testing_ya import api_wrappers


class WaitTimeoutError(Exception):
    pass


class FileUploaderApiClient(request_executors_base.BaseRequestExecutor):
    async def post_file(self, data: bytes, file_name: str) -> api_wrappers.Resp:
        with aiohttp.MultipartWriter() as multipart_writer:
            payload = multipart_writer.append(data)
            payload.set_content_disposition("form-data", name="file", filename=file_name)

        request = api_wrappers.Req(
            method="post",
            url="/api/v2/files",
            data=multipart_writer,
        )
        return await self._request(request)

    async def get_file_status(self, file_id: str) -> api_wrappers.Resp:
        request = api_wrappers.Req(method="get", url=f"/api/v2/files/{file_id}/status", require_ok=False)
        return await self._request(request)

    async def wait_for_file_status(
        self, file_id: str, timeout_seconds: float = 100, retry_delay_seconds: float = 1
    ) -> api_wrappers.Resp:
        deadline = time.time() + timeout_seconds

        while True:
            if time.time() > deadline:
                raise WaitTimeoutError("File upload wait reached timeout")

            response = await self.get_file_status(file_id=file_id)
            if response.status == 404 or response.json["status"] != "ready":
                self.logger.row(f"Waiting for file upload status: {response.json}")
                await asyncio.sleep(retry_delay_seconds)
                continue

            return response

    async def get_file_sources(self, file_id: str) -> api_wrappers.Resp:
        request = api_wrappers.Req(
            method="get",
            url=f"/api/v2/files/{file_id}/sources",
        )
        return await self._request(request)


__all__ = [
    "FileUploaderApiClient",
]
