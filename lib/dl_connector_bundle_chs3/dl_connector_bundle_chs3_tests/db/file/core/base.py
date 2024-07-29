import uuid

import pytest

from dl_constants.enums import FileProcessingStatus
from dl_core_testing.fixtures.primitives import FixtureTableSpec

from dl_connector_bundle_chs3.file.core.constants import (
    CONNECTION_TYPE_FILE,
    SOURCE_TYPE_FILE_S3_TABLE,
)
from dl_connector_bundle_chs3.file.core.us_connection import FileS3Connection
from dl_connector_bundle_chs3_tests.db.base.core.base import BaseCHS3TestClass


class BaseFileS3TestClass(BaseCHS3TestClass[FileS3Connection]):
    conn_type = CONNECTION_TYPE_FILE
    source_type = SOURCE_TYPE_FILE_S3_TABLE

    @pytest.fixture(scope="function")
    def sample_file_data_source(
        self,
        sample_table_spec: FixtureTableSpec,
        sample_s3_file: str,
    ) -> FileS3Connection.FileDataSource:
        raw_schema = self._get_raw_schema_for_ch_table(sample_table_spec)
        return FileS3Connection.FileDataSource(
            id=str(uuid.uuid4()),
            file_id=str(uuid.uuid4()),
            title=sample_s3_file,
            s3_filename_suffix=sample_s3_file,
            raw_schema=raw_schema,
            status=FileProcessingStatus.ready,
            column_types=[{"name": col[0], "user_type": col[1].name} for col in sample_table_spec.table_schema],
        )
