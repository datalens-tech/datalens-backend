from __future__ import annotations

import marshmallow as ma
from marshmallow_enum import EnumField

from bi_core.marshmallow import ErrorCodeField

from bi_file_uploader_lib.enums import ErrorLevel, ErrorObjectKind
from bi_file_uploader_lib.exc import GLOBAL_ERR_PREFIX

from bi_file_uploader_api_lib.schemas.base import BaseRequestSchema


class ErrorInfoSchema(BaseRequestSchema):
    code = ma.fields.String()
    object_kind = EnumField(enum=ErrorObjectKind)
    object_id = ma.fields.String()
    message = ma.fields.String()
    details = ma.fields.Dict(keys=ma.fields.String())
    level = EnumField(enum=ErrorLevel)


class FileProcessingErrorApiSchema(ma.Schema):
    level = EnumField(ErrorLevel)
    message = ma.fields.String()
    code = ErrorCodeField(prefix=(GLOBAL_ERR_PREFIX,))
    details = ma.fields.Dict()
