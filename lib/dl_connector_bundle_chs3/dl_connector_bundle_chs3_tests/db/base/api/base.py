import abc
import datetime
import math
from typing import Generator

import attr
import pytest

from dl_api_client.dsmaker.api.http_sync_base import SyncHttpClientBase
from dl_api_lib_testing.configuration import ApiTestEnvironmentConfiguration
from dl_api_lib_testing.connection_base import ConnectionTestBase
from dl_configs.connectors_settings import ConnectorSettingsBase
from dl_constants.enums import (
    ConnectionType,
    UserDataType,
)
from dl_core.services_registry.file_uploader_client_factory import (
    FileSourceDesc,
    FileUploaderClient,
    FileUploaderClientFactory,
    SourceInternalParams,
    SourcePreview,
)
from dl_core.us_manager.us_manager_sync import SyncUSManager
from dl_core_testing.database import (
    C,
    make_sample_data,
)

from dl_connector_bundle_chs3.chs3_base.core.us_connection import BaseFileS3Connection
from dl_connector_bundle_chs3_tests.db.base.core.base import (
    FILE_CONN_TV,
    BaseCHS3TestClass,
)
from dl_connector_bundle_chs3_tests.db.config import API_TEST_CONFIG


@attr.s
class FileUploaderClientMockup(FileUploaderClient):
    # In reality, we already receive json-serialized values for the preview from the source,
    # so tune value generators here as well
    _VALUE_GENERATORS = {
        UserDataType.date: lambda rn, ts, **kwargs: (ts.date() + datetime.timedelta(days=rn)).isoformat(),
        UserDataType.datetime: lambda rn, ts, **kwargs: (ts + datetime.timedelta(days=rn / math.pi)).isoformat(),
        UserDataType.genericdatetime: lambda rn, ts, **kwargs: (ts + datetime.timedelta(days=rn / math.pi)).isoformat(),
    }

    async def get_preview(self, src: FileSourceDesc) -> SourcePreview:
        if src.raw_schema is None:
            return SourcePreview(source_id=src.source_id, preview=[])
        cols = [
            C(sch_col.name, sch_col.user_type, sch_col.nullable, vg=self._VALUE_GENERATORS.get(sch_col.user_type))
            for sch_col in src.raw_schema
        ]
        preview_dicts = make_sample_data(cols, rows=20)
        preview = [[row[c.name] for c in cols] for row in preview_dicts]
        return SourcePreview(source_id=src.source_id, preview=preview)

    async def get_internal_params(self, src: FileSourceDesc) -> SourceInternalParams:
        """Should normally return actual connection params from US, but this will do for tests"""

        return SourceInternalParams(
            preview_id=src.preview_id,
            raw_schema=src.raw_schema,
        )


class CHS3ConnectionApiTestBase(BaseCHS3TestClass[FILE_CONN_TV], ConnectionTestBase, metaclass=abc.ABCMeta):
    bi_compeng_pg_on = False

    @pytest.fixture(scope="class")
    def bi_test_config(self) -> ApiTestEnvironmentConfiguration:
        return API_TEST_CONFIG

    @pytest.fixture(scope="class")
    def monkeyclass(self) -> Generator[pytest.MonkeyPatch, None, None]:
        with pytest.MonkeyPatch.context() as mp:
            yield mp

    @pytest.fixture(scope="class", autouse=True)
    def patch_file_uploader_client(self, monkeyclass: pytest.MonkeyPatch) -> None:
        monkeyclass.setattr(FileUploaderClientFactory, "_file_uploader_client_cls", FileUploaderClientMockup)

    @pytest.fixture(scope="class")
    def connectors_settings(self) -> dict[ConnectionType, ConnectorSettingsBase]:
        return {self.conn_type: self.connection_settings}

    @pytest.fixture(scope="function")
    def saved_connection_id(
        self,
        control_api_sync_client: SyncHttpClientBase,
        connection_params: dict,
        sync_us_manager: SyncUSManager,
        sample_file_data_source: BaseFileS3Connection.FileDataSource,
    ) -> Generator[str, None, None]:
        """
        Normally connections are updated by file uploader worker,
        but that would require the whole file-uploader pipeline to be setup in tests
        """

        with super().create_connection(
            control_api_sync_client=control_api_sync_client,
            connection_params=connection_params,
        ) as conn_id:
            conn = sync_us_manager.get_by_id(conn_id, BaseFileS3Connection)
            for src in conn.data.sources:
                src.status = sample_file_data_source.status
                src.raw_schema = sample_file_data_source.raw_schema
                src.s3_filename = sample_file_data_source.s3_filename
            sync_us_manager.save(conn)
            yield conn_id

    @abc.abstractmethod
    @pytest.fixture(scope="function")
    def connection_params(self, sample_file_data_source: BaseFileS3Connection.FileDataSource) -> dict:
        raise NotImplementedError()
