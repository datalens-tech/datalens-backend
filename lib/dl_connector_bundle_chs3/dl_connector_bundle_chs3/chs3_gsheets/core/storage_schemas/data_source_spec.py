from dl_connector_bundle_chs3.chs3_base.core.storage_schemas.data_source_spec import (
    BaseFileS3DataSourceSpecStorageSchema,
)
from dl_connector_bundle_chs3.chs3_gsheets.core.data_source_spec import GSheetsFileS3DataSourceSpec


class GSheetsFileS3DataSourceSpecStorageSchema(BaseFileS3DataSourceSpecStorageSchema):
    TARGET_CLS = GSheetsFileS3DataSourceSpec
