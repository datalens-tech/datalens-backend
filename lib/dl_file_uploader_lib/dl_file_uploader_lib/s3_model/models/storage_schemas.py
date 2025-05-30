from marshmallow import fields

from dl_file_uploader_lib.s3_model.base import S3BaseModelSchema
from dl_file_uploader_lib.s3_model.models import S3DataSourcePreview


class S3DataSourcePreviewSchema(S3BaseModelSchema):
    class Meta(S3BaseModelSchema.Meta):
        target = S3DataSourcePreview

    preview_data = fields.List(fields.List(fields.Raw(allow_none=True)))
