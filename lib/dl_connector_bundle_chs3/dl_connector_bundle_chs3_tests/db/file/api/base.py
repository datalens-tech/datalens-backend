import pytest

from dl_connector_bundle_chs3.file.core.us_connection import FileS3Connection
from dl_connector_bundle_chs3_tests.db.base.api.base import CHS3ConnectionApiTestBase
from dl_connector_bundle_chs3_tests.db.base.api.data import CHS3DataApiTestBase
from dl_connector_bundle_chs3_tests.db.base.api.dataset import CHS3DatasetTestBase
from dl_connector_bundle_chs3_tests.db.file.core.base import BaseFileS3TestClass


class FileS3ApiConnectionTestBase(
    BaseFileS3TestClass,
    CHS3ConnectionApiTestBase[FileS3Connection],
):
    @pytest.fixture(scope="function")
    def connection_params(
        self,
        sample_file_data_source: FileS3Connection.FileDataSource,
    ) -> dict:
        return dict(
            sources=[
                dict(
                    file_id=sample_file_data_source.file_id,
                    id=sample_file_data_source.id,
                    title=sample_file_data_source.title,
                    column_types=sample_file_data_source.column_types,
                ),
            ],
        )


class FileS3DatasetTestBase(FileS3ApiConnectionTestBase, CHS3DatasetTestBase[FileS3Connection]):
    pass


class FileS3DataApiTestBase(FileS3DatasetTestBase, CHS3DataApiTestBase[FileS3Connection]):
    pass
