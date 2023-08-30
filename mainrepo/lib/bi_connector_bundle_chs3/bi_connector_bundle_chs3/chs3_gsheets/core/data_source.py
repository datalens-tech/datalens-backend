from __future__ import annotations

from bi_constants.enums import CreateDSFrom

from bi_connector_bundle_chs3.chs3_gsheets.core.constants import CONNECTION_TYPE_GSHEETS_V2, SOURCE_TYPE_GSHEETS_V2
from bi_connector_bundle_chs3.chs3_base.core.data_source import BaseFileS3DataSource


class GSheetsFileS3DataSource(BaseFileS3DataSource):
    conn_type = CONNECTION_TYPE_GSHEETS_V2

    @classmethod
    def is_compatible_with_type(cls, source_type: CreateDSFrom) -> bool:
        return source_type in {
            SOURCE_TYPE_GSHEETS_V2,
        }
