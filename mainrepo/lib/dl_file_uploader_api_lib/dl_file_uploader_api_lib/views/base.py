from __future__ import annotations

from typing import (
    Any,
    ClassVar,
    Type,
)

from marshmallow import Schema

from dl_api_commons.aiohttp.aiohttp_wrappers import (
    DLRequestView,
    RequiredResource,
    RequiredResourceCommon,
)
from dl_file_uploader_api_lib.dl_request import FileUploaderDLRequest


class FileUploaderBaseView(DLRequestView[FileUploaderDLRequest]):
    dl_request_cls = FileUploaderDLRequest

    REQUIRED_RESOURCES: ClassVar[frozenset[RequiredResource]] = frozenset({RequiredResourceCommon.SKIP_CSRF})

    async def _load_post_request_schema_data(self, schema_cls: Type[Schema]) -> dict[str, Any]:
        json_data = await self.request.json() if self.request.can_read_body else {}
        req_data = schema_cls().load(
            {
                **self.request.match_info,
                **json_data,
            }
        )
        return req_data
