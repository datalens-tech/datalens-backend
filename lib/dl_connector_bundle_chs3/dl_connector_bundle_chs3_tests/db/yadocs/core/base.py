import uuid

import pytest

from dl_constants.enums import FileProcessingStatus
from dl_core_testing.fixtures.primitives import FixtureTableSpec

from dl_connector_bundle_chs3.chs3_yadocs.core.constants import (
    CONNECTION_TYPE_YADOCS,
    SOURCE_TYPE_YADOCS,
)
from dl_connector_bundle_chs3.chs3_yadocs.core.us_connection import YaDocsFileS3Connection
from dl_connector_bundle_chs3_tests.db.base.core.base import BaseCHS3TestClass


class BaseYaDocsFileS3TestClass(BaseCHS3TestClass[YaDocsFileS3Connection]):
    conn_type = CONNECTION_TYPE_YADOCS
    source_type = SOURCE_TYPE_YADOCS

    @pytest.fixture(scope="function")
    def sample_file_data_source(
        self,
        sample_table_spec: FixtureTableSpec,
        sample_s3_file: str,
    ) -> YaDocsFileS3Connection.FileDataSource:
        raw_schema = self._get_raw_schema_for_ch_table(sample_table_spec)
        return YaDocsFileS3Connection.FileDataSource(
            id=str(uuid.uuid4()),
            file_id=str(uuid.uuid4()),
            title=sample_s3_file,
            s3_filename_suffix=sample_s3_file,
            raw_schema=raw_schema,
            status=FileProcessingStatus.ready,
        )
