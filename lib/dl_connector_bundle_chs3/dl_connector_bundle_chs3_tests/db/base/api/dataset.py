import abc
from http import HTTPStatus
import uuid

import pytest

from dl_api_client.dsmaker.api.dataset_api import SyncHttpDatasetApiV1
from dl_api_client.dsmaker.primitives import Dataset
from dl_api_lib_testing.connector.dataset_suite import DefaultConnectorDatasetTestSuite
from dl_api_lib_testing.dataset_base import DatasetTestBase
from dl_constants.enums import (
    DataSourceRole,
    FileProcessingStatus,
)
from dl_core.us_manager.us_manager_sync import SyncUSManager

from dl_connector_bundle_chs3.chs3_base.core.us_connection import BaseFileS3Connection
from dl_connector_bundle_chs3_tests.db.base.api.base import CHS3ConnectionApiTestBase
from dl_connector_bundle_chs3_tests.db.base.core.base import FILE_CONN_TV


class CHS3DatasetTestBase(CHS3ConnectionApiTestBase[FILE_CONN_TV], DatasetTestBase, metaclass=abc.ABCMeta):
    @pytest.fixture(scope="function")
    def dataset_params(self, sample_file_data_source) -> dict:
        return dict(
            source_type=self.source_type.name,
            parameters=dict(
                origin_source_id=sample_file_data_source.id,
            ),
        )


class CHS3DatasetTestSuite(CHS3DatasetTestBase, DefaultConnectorDatasetTestSuite, metaclass=abc.ABCMeta):
    @pytest.mark.parametrize(
        "source_status, expected_status",
        (
            (FileProcessingStatus.ready, HTTPStatus.OK),
            (FileProcessingStatus.in_progress, HTTPStatus.BAD_REQUEST),
            (FileProcessingStatus.failed, HTTPStatus.BAD_REQUEST),
        ),
    )
    def test_add_dataset_source(
        self,
        sync_us_manager: SyncUSManager,
        saved_connection: FILE_CONN_TV,
        sample_file_data_source: BaseFileS3Connection.FileDataSource,
        control_api: SyncHttpDatasetApiV1,
        source_status: FileProcessingStatus,
        expected_status: int,
    ) -> None:
        saved_connection.update_data_source(
            id=sample_file_data_source.id,
            role=DataSourceRole.origin,
            status=source_status,
        )
        sync_us_manager.save(saved_connection)

        ds = Dataset()
        ds.sources["source_1"] = ds.source(
            source_type=self.source_type,
            connection_id=saved_connection.uuid,
            parameters=dict(
                origin_source_id=sample_file_data_source.id,
            ),
        )
        ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()
        ds_resp = control_api.apply_updates(dataset=ds, fail_ok=True)
        assert ds_resp.status_code == expected_status, ds_resp.json

        ds = ds_resp.dataset
        ds_resp = control_api.save_dataset(dataset=ds)
        assert ds_resp.status_code == HTTPStatus.OK, ds_resp.response_errors

    def test_table_name_spoofing(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_connection_id: str,
    ) -> None:
        fake_parameters = {
            "db_name": "fake db_name",
            "db_version": "fake db_version",
            "table_name": "hack_me.native",
            "origin_source_id": "fake_source_id",
        }

        ds = Dataset()
        ds.sources["source_1"] = ds.source(
            source_type=self.source_type,
            connection_id=saved_connection_id,
            parameters=fake_parameters,
        )
        ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()
        ds_resp = control_api.apply_updates(dataset=ds, fail_ok=True)
        assert ds_resp.status_code == HTTPStatus.BAD_REQUEST, ds_resp.json

    def test_update_connection_source(
        self,
        sync_us_manager: SyncUSManager,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
        saved_connection_id: str,
        sample_file_data_source: BaseFileS3Connection.FileDataSource,
    ) -> None:
        orig_ds = saved_dataset

        # --- change connection source: add one column ---
        conn = sync_us_manager.get_by_id(saved_connection_id, BaseFileS3Connection)
        new_field = sample_file_data_source.raw_schema[-1].clone(name="new_field")
        conn.update_data_source(
            id=sample_file_data_source.id,
            role=DataSourceRole.origin,
            raw_schema=sample_file_data_source.raw_schema + [new_field],
        )
        sync_us_manager.save(conn)
        # --- ---------------------------------------- ---

        refresh_resp = control_api.refresh_dataset_sources(orig_ds, [orig_ds.sources[0].id])
        assert refresh_resp.status_code == HTTPStatus.OK, refresh_resp.json
        ds = refresh_resp.dataset

        old_raw_schema, new_raw_schema = orig_ds.sources[0].raw_schema, ds.sources[0].raw_schema
        assert len(new_raw_schema) == len(old_raw_schema) + 1

    def test_replace_connection(
        self,
        sync_us_manager: SyncUSManager,
        saved_dataset: Dataset,
        saved_connection_2: FILE_CONN_TV,
        control_api: SyncHttpDatasetApiV1,
    ) -> None:
        new_connection = saved_connection_2

        ds = saved_dataset

        # 1. Replace connection
        replace_conn_resp = control_api.replace_connection(
            dataset=ds,
            new_connection_id=new_connection.uuid,
            fail_ok=True,
        )
        assert replace_conn_resp.status_code == HTTPStatus.BAD_REQUEST, replace_conn_resp.response_errors
        # ^ there is no seamless migrator for file connections,
        # so manual source replacement is necessary to complete the connection replacement
        ds = replace_conn_resp.dataset

        # 2. Manually replace datasource
        sources = control_api.get_connection_sources(new_connection.uuid).json["sources"]
        new_source_id = str(uuid.uuid4())
        replace_src_resp = control_api.replace_single_data_source(
            dataset=ds,
            new_source={
                "id": new_source_id,
                **sources[0],
            },
        )

        assert replace_src_resp.status_code == HTTPStatus.OK, replace_src_resp.response_errors
