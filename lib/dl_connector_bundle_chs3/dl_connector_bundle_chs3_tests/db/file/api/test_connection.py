import uuid

import pytest

from dl_connector_bundle_chs3.file.core.us_connection import FileS3Connection
from dl_connector_bundle_chs3_tests.db.base.api.connection import CHS3ConnectionTestSuite
from dl_connector_bundle_chs3_tests.db.file.api.base import FileS3ApiConnectionTestBase


class TestFileS3Connection(FileS3ApiConnectionTestBase, CHS3ConnectionTestSuite[FileS3Connection]):
    @pytest.fixture(scope="function")
    def single_new_conn_source_params(self) -> dict:
        return {
            "id": str(uuid.uuid4()),
            "file_id": str(uuid.uuid4()),
            "title": f"New File {str(uuid.uuid4())}",
            "column_types": [
                {"name": "new_field", "user_type": "string"},
            ],
        }
