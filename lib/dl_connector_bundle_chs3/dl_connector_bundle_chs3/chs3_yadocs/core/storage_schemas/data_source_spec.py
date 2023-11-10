from dl_connector_bundle_chs3.chs3_base.core.storage_schemas.data_source_spec import (
    BaseFileS3DataSourceSpecStorageSchema,
)
from dl_connector_bundle_chs3.chs3_yadocs.core.data_source_spec import YaDocsFileS3DataSourceSpec


class YaDocsFileS3DataSourceSpecStorageSchema(BaseFileS3DataSourceSpecStorageSchema):
    TARGET_CLS = YaDocsFileS3DataSourceSpec
