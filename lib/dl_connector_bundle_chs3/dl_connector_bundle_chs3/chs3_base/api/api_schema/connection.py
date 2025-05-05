from __future__ import annotations

from typing import Any

from marshmallow import (
    fields,
    post_dump,
    pre_dump,
    validate,
)

from dl_api_connector.api_schema.component_errors import ComponentErrorListSchema
from dl_api_connector.api_schema.connection_base import ConnectionSchema
from dl_api_connector.api_schema.extras import FieldExtra
from dl_constants.exc import (
    DEFAULT_ERR_CODE_API_PREFIX,
    GLOBAL_ERR_PREFIX,
)
from dl_core import exc
from dl_core.flask_utils.us_manager_middleware import USManagerFlaskMiddleware

from dl_connector_bundle_chs3.chs3_base.api.api_schema.source import (
    BaseFileSourceSchema,
    ReplaceFileSourceSchema,
)
from dl_connector_bundle_chs3.chs3_base.core.us_connection import BaseFileS3Connection


class BaseFileS3ConnectionSchema(ConnectionSchema):
    TARGET_CLS = BaseFileS3Connection

    sources = fields.Nested(
        BaseFileSourceSchema,
        many=True,
        attribute="data.sources",
        bi_extra=FieldExtra(editable=True),
        validate=validate.Length(min=1, max=10),
    )
    replace_sources = fields.Nested(
        ReplaceFileSourceSchema,
        many=True,
        load_default=[],
        attribute="data.replace_sources",
        bi_extra=FieldExtra(editable=True),
        load_only=True,
    )

    component_errors = fields.Nested(ComponentErrorListSchema, attribute="data.component_errors", required=False)

    def create_data_model_constructor_kwargs(self, data_attributes: dict[str, Any]) -> dict[str, Any]:
        base_kwargs = super().create_data_model_constructor_kwargs(data_attributes)
        base_kwargs.pop("replace_sources")  # TODO
        return base_kwargs

    @post_dump(pass_original=True)
    def load_preview(self, data: dict[str, Any], conn: BaseFileS3Connection, **kwargs: Any) -> dict[str, Any]:
        # TODO: read preview data from s3 if status == FileProcessingStatus.ready ???

        usm = USManagerFlaskMiddleware.get_request_service_us_manager()
        service_registry = usm.get_services_registry()
        fu_client_factory = service_registry.get_file_uploader_client_factory()

        file_sources = [src.get_desc() for src in conn.data.sources]
        with fu_client_factory.get_client() as fu_client:
            sources_preview = fu_client.get_preview_batch_sync(file_sources)
        preview_by_source_id = {sp.source_id: sp.preview for sp in sources_preview}

        for source in data["sources"]:
            source["preview"] = preview_by_source_id[source["id"]]

        return data

    @pre_dump
    def expand_source_errors(self, conn: BaseFileS3Connection, **kwargs: Any) -> BaseFileS3Connection:
        expected_prefix = [GLOBAL_ERR_PREFIX, DEFAULT_ERR_CODE_API_PREFIX] + exc.DataSourceError.err_code
        for err_pack in conn.data.component_errors.items:
            for error in err_pack.errors:
                if len(error.code) < len(expected_prefix) or error.code[: len(expected_prefix)] != expected_prefix:
                    error.code = expected_prefix + error.code
        return conn
