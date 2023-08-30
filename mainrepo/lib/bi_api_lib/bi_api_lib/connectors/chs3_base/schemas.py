from __future__ import annotations

from typing import Any, Type

import marshmallow as ma

from bi_api_lib.error_handling import GLOBAL_ERR_PREFIX, DEFAULT_ERR_CODE_API_PREFIX
from bi_api_lib.schemas.dataset_base import ComponentErrorListSchema
from marshmallow import fields, Schema, RAISE, validate, post_load, post_dump, pre_dump

from bi_api_commons.flask.middlewares.commit_rci_middleware import ReqCtxInfoMiddleware
from bi_constants.enums import BIType, FileProcessingStatus
from bi_connector_bundle_chs3.chs3_base.core.us_connection import BaseFileS3Connection
from bi_core.flask_utils.us_manager_middleware import USManagerFlaskMiddleware
from bi_core.utils import make_user_auth_headers, make_user_auth_cookies
from bi_core import exc

from bi_model_tools.schema.base import BaseSchema
from bi_api_connector.api_schema.connection_base import ConnectionSchema
from bi_api_connector.api_schema.extras import FieldExtra
from bi_api_connector.api_schema.source_base import SQLDataSourceSchema, SQLDataSourceTemplateSchema


class FileSourceColumnTypeSchema(Schema):
    class Meta:
        unknown = RAISE

    name = fields.String()
    user_type = fields.Enum(BIType)


class RawSchemaColumnSchema(Schema):
    name = fields.String()
    title = fields.String()
    user_type = fields.Enum(BIType)


class BaseFileSourceSchema(Schema):
    class Meta:
        unknown = RAISE

        target: Type[BaseFileS3Connection.FileDataSource]

    @post_load(pass_many=False)
    def to_object(self, data: dict[str, Any], **kwargs: Any) -> BaseFileS3Connection.FileDataSource:
        return self.Meta.target(**data)

    id = fields.String()
    file_id = fields.String(load_default=None)
    title = fields.String(bi_extra=FieldExtra(editable=True))
    status = fields.Enum(FileProcessingStatus, dump_only=True)
    raw_schema = fields.Nested(
        RawSchemaColumnSchema,
        many=True,
        dump_only=True,
    )
    preview = fields.List(fields.List(fields.Raw), dump_only=True, dump_default=None)
    # meta_info = {"row_num": 100500, "size": 256, "file_name": 'file_name.csv', }


class ReplaceFileSourceSchema(Schema):
    class Meta:
        unknown = RAISE

    old_source_id = fields.String()
    new_source_id = fields.String()


class BaseFileS3ConnectionSchema(ConnectionSchema):
    TARGET_CLS = BaseFileS3Connection

    sources = fields.Nested(
        BaseFileSourceSchema,
        many=True,
        attribute='data.sources',
        bi_extra=FieldExtra(editable=True),
        validate=validate.Length(min=1, max=10),
    )
    replace_sources = fields.Nested(
        ReplaceFileSourceSchema,
        many=True,
        load_default=[],
        attribute='data.replace_sources',
        bi_extra=FieldExtra(editable=True),
        load_only=True,
    )

    component_errors = fields.Nested(ComponentErrorListSchema, attribute='data.component_errors', required=False)

    # @validates_schema
    # def validate_sources(self, data: Optional[dict], *_args: Any, **_kwargs: Any) -> None:
    #     if data is None:
    #         return

    def create_data_model_constructor_kwargs(self, data_attributes: dict[str, Any]) -> dict[str, Any]:
        base_kwargs = super().create_data_model_constructor_kwargs(data_attributes)
        base_kwargs.pop('replace_sources')  # TODO
        return base_kwargs

    @post_dump(pass_original=True)
    def load_preview(self, data: dict[str, Any], conn: BaseFileS3Connection, **kwargs: Any) -> dict[str, Any]:

        # TODO: read preview data from s3 if status == FileProcessingStatus.ready ???

        rci = ReqCtxInfoMiddleware.get_request_context_info()
        headers = make_user_auth_headers(rci=rci)
        cookies = make_user_auth_cookies(rci=rci)

        usm = USManagerFlaskMiddleware.get_request_service_us_manager()
        service_registry = usm.get_services_registry()
        fu_client_factory = service_registry.get_file_uploader_client_factory()

        file_sources = [src.get_desc() for src in conn.data.sources]
        with fu_client_factory.get_client(headers=headers, cookies=cookies) as fu_client:
            sources_preview = fu_client.get_preview_batch_sync(file_sources)
        preview_by_source_id = {sp.source_id: sp.preview for sp in sources_preview}

        for source in data['sources']:
            source['preview'] = preview_by_source_id[source['id']]

        return data

    @pre_dump
    def expand_source_errors(self, conn: BaseFileS3Connection, **kwargs: Any) -> BaseFileS3Connection:
        expected_prefix = [GLOBAL_ERR_PREFIX, DEFAULT_ERR_CODE_API_PREFIX] + exc.DataSourceError.err_code
        for err_pack in conn.data.component_errors.items:
            for error in err_pack.errors:
                if len(error.code) < len(expected_prefix) or error.code[:len(expected_prefix)] != expected_prefix:
                    error.code = expected_prefix + error.code
        return conn


class BaseFileS3DataSourceParametersSchema(BaseSchema):
    origin_source_id = ma.fields.String(allow_none=True, load_default=None)


class BaseFileS3DataSourceSchema(SQLDataSourceSchema):
    parameters = ma.fields.Nested(BaseFileS3DataSourceParametersSchema)


class BaseFileS3DataSourceTemplateSchema(SQLDataSourceTemplateSchema):
    parameters = ma.fields.Nested(BaseFileS3DataSourceParametersSchema)
