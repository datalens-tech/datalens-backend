import uuid

import pytest

from dl_constants.enums import FileProcessingStatus
from dl_core.us_manager.us_manager_sync import SyncUSManager
from dl_core_testing.fixtures.primitives import FixtureTableSpec

from dl_connector_bundle_chs3.chs3_gsheets.core.constants import (
    CONNECTION_TYPE_GSHEETS_V2,
    SOURCE_TYPE_GSHEETS_V2,
)
from dl_connector_bundle_chs3.chs3_gsheets.core.testing.connection import make_saved_gsheets_v2_connection
from dl_connector_bundle_chs3.chs3_gsheets.core.us_connection import GSheetsFileS3Connection
from dl_connector_bundle_chs3_tests.db.base.core.base import BaseCHS3TestClass


class BaseGSheetsFileS3TestClass(BaseCHS3TestClass[GSheetsFileS3Connection]):
    conn_type = CONNECTION_TYPE_GSHEETS_V2
    source_type = SOURCE_TYPE_GSHEETS_V2

    @pytest.fixture(scope="function")
    def sample_file_data_source(
        self,
        sample_table_spec: FixtureTableSpec,
        sample_s3_file: str,
    ) -> GSheetsFileS3Connection.FileDataSource:
        raw_schema = self._get_raw_schema_for_ch_table(sample_table_spec)
        return GSheetsFileS3Connection.FileDataSource(
            id=str(uuid.uuid4()),
            file_id=str(uuid.uuid4()),
            title=sample_s3_file,
            s3_filename_suffix=sample_s3_file,
            raw_schema=raw_schema,
            status=FileProcessingStatus.ready,
        )

    @pytest.fixture(scope="function")
    def saved_connection(
        self,
        sync_us_manager: SyncUSManager,
        connection_creation_params: dict,
    ) -> GSheetsFileS3Connection:
        conn = make_saved_gsheets_v2_connection(
            sync_usm=sync_us_manager,
            **connection_creation_params,
        )
        return conn
