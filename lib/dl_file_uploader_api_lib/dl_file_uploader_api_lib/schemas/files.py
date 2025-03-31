from __future__ import annotations

from typing import (
    Any,
)

import marshmallow as ma
from marshmallow_oneofschema import OneOfSchema

from dl_constants.enums import FileProcessingStatus
from dl_file_uploader_api_lib.schemas.base import BaseRequestSchema
from dl_file_uploader_api_lib.schemas.errors import (
    ErrorInfoSchema,
    FileProcessingErrorApiSchema,
)
from dl_file_uploader_lib import exc
from dl_file_uploader_lib.enums import FileType


def validate_authorized_gsheets(data: dict) -> None:
    if data["authorized"] and data["refresh_token"] is None and data["connection_id"] is None:
        raise ma.ValidationError(
            "Either refresh_token or connection_id must be provided when authorized is true",
            "authorized",
        )


def validate_authorized_yadocs(data: dict) -> None:
    if data["authorized"] and data["oauth_token"] is None and data["connection_id"] is None:
        raise ma.ValidationError(
            "Either oauth_token or connection_id must be provided when authorized is true",
            "authorized",
        )


def validate_yadocs_data(data: dict) -> None:
    if not ((data["public_link"] is None) ^ (data["private_path"] is None)):
        raise ValueError("Expected exactly one of [`private_path`, `public_link`] to be specified")
    if data["public_link"] is None and data["oauth_token"] is None and data["connection_id"] is None:
        raise ma.ValidationError(
            "Authentication is required to work with private files (expected `oauth_token` or `connection_id` to be specified)"
        )


class FileLinkRequestSchema(BaseRequestSchema):
    type = ma.fields.Enum(FileType, required=True)
    url = ma.fields.String(required=True)
    refresh_token = ma.fields.String(load_default=None, allow_none=True)
    connection_id = ma.fields.String(load_default=None, allow_none=True)
    authorized = ma.fields.Boolean(required=True)

    @ma.validates_schema(skip_on_field_errors=True)
    def validate_object(self, data: dict, **kwargs: Any) -> None:
        validate_authorized_gsheets(data)


class FileDocumentsRequestSchema(BaseRequestSchema):
    connection_id = ma.fields.String(load_default=None, allow_none=True)
    private_path = ma.fields.String(load_default=None, allow_none=True)
    oauth_token = ma.fields.String(load_default=None, allow_none=True)
    public_link = ma.fields.String(load_default=None, allow_none=True)
    authorized = ma.fields.Boolean(required=True)

    @ma.validates_schema(skip_on_field_errors=True)
    def validate_object(self, data: dict, **kwargs: Any) -> None:
        validate_yadocs_data(data)


class FileUploadResponseSchema(ma.Schema):
    file_id = ma.fields.String()
    title = ma.fields.String()


class MakePresignedUrlRequestSchema(ma.Schema):
    content_md5 = ma.fields.String(required=True)


class DownloadPresignedUrlRequestSchema(ma.Schema):
    filename = ma.fields.String(required=True)
    key = ma.fields.String(required=True)


class PresignedUrlSchema(ma.Schema):
    class PresignedUrlFields(ma.Schema):
        class Meta:
            unknown = ma.INCLUDE

        key = ma.fields.String()
        x_amz_algorithm = ma.fields.String(attribute="x-amz-algorithm", data_key="x-amz-algorithm")
        x_amz_credential = ma.fields.String(attribute="x-amz-credential", data_key="x-amz-credential")
        x_amz_date = ma.fields.String(attribute="x-amz-date", data_key="x-amz-date")
        policy = ma.fields.String()
        x_amz_signature = ma.fields.String(attribute="x-amz-signature", data_key="x-amz-signature")

    url = ma.fields.String(required=True)
    _fields = ma.fields.Nested(PresignedUrlFields, required=True, attribute="fields", data_key="fields")


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


class UpdateConnectionDataSourceSchemaBase(BaseRequestSchema):
    id = ma.fields.String()
    title = ma.fields.String()
    first_line_is_header = ma.fields.Boolean(load_default=None)


class UpdateConnectionDataSourceSchemaGSheets(UpdateConnectionDataSourceSchemaBase):
    spreadsheet_id = ma.fields.String(load_default=None)
    sheet_id = ma.fields.Integer(load_default=None)


class UpdateConnectionDataSourceSchemaYaDocs(UpdateConnectionDataSourceSchemaBase):
    public_link = ma.fields.String(load_default=None)
    private_path = ma.fields.String(load_default=None)
    sheet_id = ma.fields.String(load_default=None)


class UpdateConnectionDataRequestSchemaBase(BaseRequestSchema):
    type = ma.fields.Enum(FileType)
    connection_id = ma.fields.String(allow_none=True, load_default=None)
    authorized = ma.fields.Boolean(required=True)
    save = ma.fields.Boolean(load_default=False)
    tenant_id = ma.fields.String(allow_none=True, load_default=None)


class UpdateConnectionDataRequestSchemaGSheets(UpdateConnectionDataRequestSchemaBase):
    refresh_token = ma.fields.String(allow_none=True, load_default=None)
    sources = ma.fields.Nested(UpdateConnectionDataSourceSchemaGSheets, many=True)

    @ma.validates_schema(skip_on_field_errors=True)
    def validate_object(self, data: dict, **kwargs: Any) -> None:
        validate_authorized_gsheets(data)

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


class UpdateConnectionDataRequestSchemaYaDocs(UpdateConnectionDataRequestSchemaBase):
    oauth_token = ma.fields.String(allow_none=True, load_default=None)
    sources = ma.fields.Nested(UpdateConnectionDataSourceSchemaYaDocs, many=True)

    @ma.validates_schema(skip_on_field_errors=True)
    def validate_object(self, data: dict, **kwargs: Any) -> None:
        validate_authorized_yadocs(data)

        incomplete_sources: list[dict[str, str]] = []
        for src in data["sources"]:
            if (
                (src["public_link"] is None and src["private_path"] is None)
                or src["sheet_id"] is None
                or src["first_line_is_header"] is None
            ):
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


class UpdateConnectionDataRequestSchema(FileTypeOneOfSchema):
    type_schemas: dict[str, type[UpdateConnectionDataRequestSchemaBase]] = {  # type: ignore  # 2024-01-24 # TODO: Incompatible types in assignment (expression has type "dict[str, type[UpdateConnectionDataRequestSchemaBase]]", base class "OneOfSchema" defined the type as "dict[str, type[Schema]]")  [assignment]
        FileType.gsheets.name: UpdateConnectionDataRequestSchemaGSheets,
        FileType.yadocs.name: UpdateConnectionDataRequestSchemaYaDocs,
    }


class UpdateConnectionDataResultSchema(ma.Schema):
    class FileSourcesSchema(ma.Schema):
        class SingleFileSourceSchema(ma.Schema):
            source_id = ma.fields.String(attribute="id")

        file_id = ma.fields.String(attribute="id")
        sources = ma.fields.Nested(SingleFileSourceSchema, many=True)

    files = ma.fields.Nested(FileSourcesSchema, many=True)
