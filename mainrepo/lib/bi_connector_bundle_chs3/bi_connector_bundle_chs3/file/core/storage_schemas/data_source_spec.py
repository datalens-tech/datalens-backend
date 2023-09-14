from bi_connector_bundle_chs3.chs3_base.core.storage_schemas.data_source_spec import (
    BaseFileS3DataSourceSpecStorageSchema,
)
from bi_connector_bundle_chs3.file.core.data_source_spec import FileS3DataSourceSpec


class FileS3DataSourceSpecStorageSchema(BaseFileS3DataSourceSpecStorageSchema):
    TARGET_CLS = FileS3DataSourceSpec
