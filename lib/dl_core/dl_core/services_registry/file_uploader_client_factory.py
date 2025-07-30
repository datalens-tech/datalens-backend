from __future__ import annotations

import asyncio
from enum import Enum
import logging
from types import TracebackType
from typing import (
    Any,
    ClassVar,
    Optional,
    Union,
)

import aiohttp
import attr
import marshmallow as ma
from marshmallow_oneofschema import OneOfSchema

from dl_api_commons.aiohttp.aiohttp_client import (
    BIAioHTTPClient,
    TCookies,
    THeaders,
)
from dl_constants.api_constants import DLHeadersCommon
from dl_constants.enums import UserDataType
from dl_core.db.elements import SchemaColumn
from dl_type_transformer.native_type_schema import OneOfNativeTypeSchema
from dl_utils.aio import await_sync


LOGGER = logging.getLogger(__name__)


RawSchemaType = list[SchemaColumn]


class FileType(Enum):
    gsheets = "gsheets"
    yadocs = "yadocs"


class FileTypeOneOfSchema(OneOfSchema):
    class Meta:
        unknown = ma.EXCLUDE

    type_field_remove = False

    def get_obj_type(self, obj: dict[str, Any]) -> str:
        type_field = obj[self.type_field] if isinstance(obj, dict) else getattr(obj, self.type_field)
        assert isinstance(type_field, FileType)
        return type_field.name

    def get_data_type(self, data: dict):  # type: ignore  # 2024-01-30 # TODO: Function is missing a return type annotation  [no-untyped-def]
        data_type = data.get(self.type_field)
        if self.type_field not in data:
            data[self.type_field] = FileType.gsheets.value
            data_type = FileType.gsheets.value
        if self.type_field in data and self.type_field_remove:
            data.pop(self.type_field)
        return data_type


@attr.s(frozen=True, kw_only=True)
class FileSourceDesc:
    file_id: str = attr.ib()
    source_id: str = attr.ib()
    title: str = attr.ib()
    raw_schema: Optional[Union[RawSchemaType, list[dict[str, Any]]]] = attr.ib()
    preview_id: Optional[str] = attr.ib(default=None)


@attr.s(frozen=True, kw_only=True)
class GSheetsFileSourceDesc(FileSourceDesc):
    spreadsheet_id: Optional[str] = attr.ib()
    sheet_id: Optional[int] = attr.ib()
    first_line_is_header: Optional[bool] = attr.ib()


@attr.s(frozen=True, kw_only=True)
class YaDocsFileSourceDesc(FileSourceDesc):
    public_link: Optional[str] = attr.ib()
    private_path: Optional[str] = attr.ib()
    first_line_is_header: Optional[bool] = attr.ib()
    sheet_id: Optional[str] = attr.ib()


@attr.s(frozen=True)
class SourceInternalParams:
    preview_id: str = attr.ib()
    raw_schema: RawSchemaType = attr.ib()


@attr.s(frozen=True)
class SourcePreview:
    source_id: str = attr.ib()
    preview: list[list[Any]] = attr.ib()


class RawSchemaColumnSchema(ma.Schema):
    name = ma.fields.String()
    title = ma.fields.String()

    native_type = ma.fields.Nested(OneOfNativeTypeSchema, allow_none=True)

    user_type = ma.fields.Enum(UserDataType)
    description = ma.fields.String(dump_default="", allow_none=True)
    has_auto_aggregation = ma.fields.Boolean(dump_default=False, allow_none=True)
    lock_aggregation = ma.fields.Boolean(dump_default=False, allow_none=True)
    nullable = ma.fields.Boolean(dump_default=None, allow_none=True)

    @ma.post_load
    def make_column(self, data: dict, **kwargs: Any) -> SchemaColumn:
        return SchemaColumn(
            name=data["name"],
            title=data["title"],
            user_type=data["user_type"],
            native_type=data["native_type"],
            description=data.get("description", ""),
            has_auto_aggregation=data.get("has_auto_aggregation", False),
            lock_aggregation=data.get("lock_aggregation", False),
            nullable=data["nullable"],
        )


class SourcePreviewApiSchema(ma.Schema):
    preview_id = ma.fields.String()
    raw_schema = ma.fields.Nested(RawSchemaColumnSchema, many=True)


class SourceInternalParamsRequestSchema(ma.Schema):
    raw_schema = ma.fields.Nested(RawSchemaColumnSchema, many=True, allow_none=True)


class SourceInternalParamsResultSchema(SourceInternalParamsRequestSchema):
    preview_id = ma.fields.String()

    @ma.post_load
    def to_object(self, data: dict, **kwargs: Any) -> SourceInternalParams:
        return SourceInternalParams(
            preview_id=data["preview_id"],
            raw_schema=data["raw_schema"],
        )


class CleanupApiSchema(ma.Schema):
    tenant_id = ma.fields.String(required=True)


class UpdateConnectionDataSourceSchemaBase(ma.Schema):
    id = ma.fields.String(attribute="source_id")
    title = ma.fields.String()
    first_line_is_header = ma.fields.Boolean()


class UpdateConnectionDataSourceSchemaGSheets(UpdateConnectionDataSourceSchemaBase):
    spreadsheet_id = ma.fields.String(load_default=None)
    sheet_id = ma.fields.Integer(load_default=None)


class UpdateConnectionDataSourceSchemaYaDocs(UpdateConnectionDataSourceSchemaBase):
    public_link = ma.fields.String(load_default=None)
    private_path = ma.fields.String(load_default=None)
    sheet_id = ma.fields.String(load_default=None)


class UpdateConnectionDataRequestSchemaBase(ma.Schema):
    type = ma.fields.Enum(FileType)
    connection_id = ma.fields.String()
    save = ma.fields.Boolean()
    authorized = ma.fields.Boolean()
    tenant_id = ma.fields.String(allow_none=True)


class UpdateConnectionDataRequestSchemaGSheets(UpdateConnectionDataRequestSchemaBase):
    sources = ma.fields.Nested(UpdateConnectionDataSourceSchemaGSheets, many=True)


class UpdateConnectionDataRequestSchemaYaDocs(UpdateConnectionDataRequestSchemaBase):
    sources = ma.fields.Nested(UpdateConnectionDataSourceSchemaYaDocs, many=True)


class UpdateConnectionDataRequestSchema(FileTypeOneOfSchema):
    type_schemas: dict[str, type[UpdateConnectionDataRequestSchemaBase]] = {
        # type: ignore  # 2024-01-24 # TODO: Incompatible types in assignment (expression has type "dict[str, type[UpdateConnectionDataRequestSchemaBase]]", base class "OneOfSchema" defined the type as "dict[str, type[Schema]]")  [assignment]
        FileType.gsheets.name: UpdateConnectionDataRequestSchemaGSheets,
        FileType.yadocs.name: UpdateConnectionDataRequestSchemaYaDocs,
    }


@attr.s
class FileUploaderClient(BIAioHTTPClient):
    async def get_preview(self, src: FileSourceDesc) -> SourcePreview:
        path = f"api/v2/files/{src.file_id}/sources/{src.source_id}/preview"
        json_data = SourcePreviewApiSchema().dump(src)
        try:
            async with self.request("post", path=path, json_data=json_data, read_timeout_sec=5) as resp:
                resp_data = await resp.json()
                return SourcePreview(source_id=src.source_id, preview=resp_data["preview"])
        except aiohttp.ClientError:
            LOGGER.exception(f"Failed to get preview for file {src.file_id} source {src.source_id}")
            return SourcePreview(source_id=src.source_id, preview=[])

    async def get_preview_batch(self, file_sources: list[FileSourceDesc]) -> list[SourcePreview]:
        previews = await asyncio.gather(*[self.get_preview(src) for src in file_sources])
        return previews

    def get_preview_batch_sync(self, file_sources: list[FileSourceDesc]) -> list[SourcePreview]:
        return await_sync(self.get_preview_batch(file_sources))

    async def get_internal_params(self, src: FileSourceDesc) -> SourceInternalParams:
        path = f"api/v2/files/{src.file_id}/sources/{src.source_id}/internal_params"
        json_data = SourceInternalParamsRequestSchema().dump(src)
        try:
            async with self.request("post", path=path, json_data=json_data, read_timeout_sec=15) as resp:
                resp_data = await resp.json()
                internal_params = SourceInternalParamsResultSchema().load(resp_data)
                return internal_params
        except aiohttp.ClientError:
            LOGGER.exception(f"Failed to get raw_schema for file {src.file_id} source {src.source_id}")
            raise

    async def get_internal_params_batch(self, file_sources: list[FileSourceDesc]) -> list[SourceInternalParams]:
        internal_params = await asyncio.gather(*[self.get_internal_params(src) for src in file_sources])
        return internal_params

    async def cleanup_tenant(self, tenant_id: str) -> None:
        path = "api/v2/cleanup"
        json_data = CleanupApiSchema().dump(dict(tenant_id=tenant_id))
        try:
            async with self.request("post", path=path, json_data=json_data, read_timeout_sec=5):
                LOGGER.info(f"Scheduled cleanup for tenant id {tenant_id}")
        except aiohttp.ClientError:
            LOGGER.exception(f"Failed to call cleanup for tenant id {tenant_id}")
            raise

    def cleanup_tenant_sync(self, tenant_id: str) -> None:
        await_sync(self.cleanup_tenant(tenant_id))

    async def update_connection_data_internal(
        self,
        conn_id: str,
        sources: list[GSheetsFileSourceDesc | YaDocsFileSourceDesc],
        authorized: bool,
        tenant_id: Optional[str],
        file_type: FileType,
    ) -> None:
        path = "/api/v2/update_connection_data_internal"
        json_data = UpdateConnectionDataRequestSchema().dump(
            dict(
                type=file_type,
                connection_id=conn_id,
                save=True,
                sources=sources,
                authorized=authorized,
                tenant_id=tenant_id,
            )
        )
        try:
            async with self.request("post", path=path, json_data=json_data, read_timeout_sec=5):
                LOGGER.info(f"Scheduled update for connection id {conn_id}")
        except aiohttp.ClientError:
            LOGGER.exception(f"Failed to call update for connection id {conn_id}")
            raise

    def close_sync(self) -> None:
        return await_sync(self.close())

    async def __aenter__(self) -> FileUploaderClient:
        return self

    async def __aexit__(
        self, exc_type: Optional[type[BaseException]], exc_val: Optional[BaseException], exc_tb: Optional[TracebackType]
    ) -> None:
        await self.close()

    def __enter__(self) -> FileUploaderClient:
        return self

    def __exit__(
        self, exc_type: Optional[type[BaseException]], exc_val: Optional[BaseException], exc_tb: Optional[TracebackType]
    ) -> None:
        self.close_sync()


@attr.s
class FileUploaderSettings:
    base_url: str = attr.ib()
    master_token: str = attr.ib()


@attr.s
class FileUploaderClientFactory:
    _file_uploader_settings: FileUploaderSettings = attr.ib()
    _ca_data: bytes = attr.ib()
    _tenant_id: Optional[str] = attr.ib()

    _file_uploader_client_cls: ClassVar[type[FileUploaderClient]] = FileUploaderClient  # tests mockup point

    def get_client(self, headers: Optional[THeaders] = None, cookies: Optional[TCookies] = None) -> FileUploaderClient:
        full_headers = headers.copy() if headers is not None else {}
        full_headers.update(
            {
                DLHeadersCommon.FILE_UPLOADER_MASTER_TOKEN.value: self._file_uploader_settings.master_token,
            }
        )
        if self._tenant_id is not None:
            full_headers.update(
                {
                    DLHeadersCommon.TENANT_ID.value: self._tenant_id,
                }
            )

        client_cls = self._file_uploader_client_cls
        return client_cls(
            base_url=self._file_uploader_settings.base_url,
            headers=full_headers,
            cookies=cookies.copy() if cookies is not None else {},
            ca_data=self._ca_data,
        )
