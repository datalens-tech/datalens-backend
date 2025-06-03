from dl_file_uploader_lib.s3_model.base import register_s3_model_storage_schema

from .models import S3DataSourcePreview
from .storage_schemas import S3DataSourcePreviewSchema


__all__ = ("S3DataSourcePreview",)


register_s3_model_storage_schema(S3DataSourcePreview, S3DataSourcePreviewSchema)
