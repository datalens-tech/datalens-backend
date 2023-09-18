from __future__ import annotations

import marshmallow as ma

from dl_constants.exc import GLOBAL_ERR_PREFIX
from dl_core.marshmallow import ErrorCodeField
from dl_file_uploader_api_lib.schemas.base import BaseRequestSchema
from dl_file_uploader_lib.enums import (
    ErrorLevel,
    ErrorObjectKind,
)


class ErrorInfoSchema(BaseRequestSchema):
    code = ma.fields.String()
    object_kind = ma.fields.Enum(enum=ErrorObjectKind)
    object_id = ma.fields.String()
    message = ma.fields.String()
    details = ma.fields.Dict(keys=ma.fields.String())
    level = ma.fields.Enum(enum=ErrorLevel)


class FileProcessingErrorApiSchema(ma.Schema):
    level = ma.fields.Enum(ErrorLevel)
    message = ma.fields.String()
    code = ErrorCodeField(prefix=(GLOBAL_ERR_PREFIX,))
    details = ma.fields.Dict()
