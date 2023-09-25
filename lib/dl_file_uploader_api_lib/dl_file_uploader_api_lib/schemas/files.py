from __future__ import annotations

from typing import Any

import marshmallow as ma

from dl_constants.enums import FileProcessingStatus
from dl_file_uploader_api_lib.schemas.base import BaseRequestSchema
from dl_file_uploader_api_lib.schemas.errors import (
    ErrorInfoSchema,
    FileProcessingErrorApiSchema,
)
from dl_file_uploader_lib import exc
from dl_file_uploader_lib.enums import FileType


def validate_authorized(data: dict) -> None:
    if data["authorized"] and data["refresh_token"] is None and data["connection_id"] is None:
        raise ma.ValidationError(
            "Either refresh_token or connection_id must be provided when authorized is true",
            "authorized",
        )


class FileLinkRequestSchema(BaseRequestSchema):
    type = ma.fields.Enum(FileType, required=True)
    url = ma.fields.String(required=True)
    refresh_token = ma.fields.String(load_default=None, allow_none=True)
    connection_id = ma.fields.String(load_default=None, allow_none=True)
    authorized = ma.fields.Boolean(required=True)

    @ma.validates_schema(skip_on_field_errors=True)
    def validate_object(self, data: dict, **kwargs: Any) -> None:
        validate_authorized(data)


class FileUploadResponseSchema(ma.Schema):
    file_id = ma.fields.String()
    title = ma.fields.String()


class FileStatusRequestSchema(BaseRequestSchema):
    file_id = ma.fields.String(required=True)


class FileStatusResultSchema(ma.Schema):
    file_id = ma.fields.String(attribute="id")
    status = ma.fields.Enum(enum=FileProcessingStatus)
    errors = ma.fields.Nested(ErrorInfoSchema, many=True)
    error = ma.fields.Nested(FileProcessingErrorApiSchema, allow_none=True)
    # progress = ma.fields.Float()


class FileSourcesRequestSchema(BaseRequestSchema):
    file_id = ma.fields.String(required=True)


class SourceShortInfoSchema(ma.Schema):
    source_id = ma.fields.String(attribute="id")
    title = ma.fields.String()
    is_applicable = ma.fields.Boolean()
    error = ma.fields.Nested(FileProcessingErrorApiSchema, allow_none=True)


class FileSourcesResultSchema(ma.Schema):
    file_id = ma.fields.String(attribute="id")
    errors = ma.fields.Nested(ErrorInfoSchema, many=True)
    sources = ma.fields.Nested(SourceShortInfoSchema, many=True)


class UpdateConnectionDataRequestSchema(BaseRequestSchema):
    class UpdateConnectionDataSourceSchema(BaseRequestSchema):
        id = ma.fields.String()
        title = ma.fields.String()
        spreadsheet_id = ma.fields.String(load_default=None)
        sheet_id = ma.fields.Integer(load_default=None)
        first_line_is_header = ma.fields.Boolean(load_default=None)

    connection_id = ma.fields.String(allow_none=True, load_default=None)
    refresh_token = ma.fields.String(allow_none=True, load_default=None)
    authorized = ma.fields.Boolean(required=True)
    save = ma.fields.Boolean(load_default=False)
    sources = ma.fields.Nested(UpdateConnectionDataSourceSchema, many=True)
    tenant_id = ma.fields.String(allow_none=True, load_default=None)

    @ma.validates_schema(skip_on_field_errors=True)
    def validate_object(self, data: dict, **kwargs: Any) -> None:
        validate_authorized(data)

        incomplete_sources: list[dict[str, str]] = []
        for src in data["sources"]:
            if src["spreadsheet_id"] is None or src["sheet_id"] is None or src["first_line_is_header"] is None:
                incomplete_sources.append(
                    dict(
                        source_id=src["id"],
                        title=src["title"],
                    )
                )

        if incomplete_sources:
            raise exc.CannotUpdateDataError(
                details=dict(
                    incomplete_sources=incomplete_sources,
                )
            )


class UpdateConnectionDataResultSchema(ma.Schema):
    class FileSourcesSchema(ma.Schema):
        class SingleFileSourceSchema(ma.Schema):
            source_id = ma.fields.String(attribute="id")

        file_id = ma.fields.String(attribute="id")
        sources = ma.fields.Nested(SingleFileSourceSchema, many=True)

    files = ma.fields.Nested(FileSourcesSchema, many=True)
