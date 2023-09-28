import abc
from http import HTTPStatus

from flask.testing import FlaskClient
import pytest

from dl_api_client.dsmaker.api.data_api import SyncHttpDataApiV2
from dl_api_client.dsmaker.api.dataset_api import SyncHttpDatasetApiV1
from dl_api_client.dsmaker.primitives import Dataset
from dl_api_lib_testing.connector.data_api_suites import DefaultConnectorDataResultTestSuite
from dl_api_lib_testing.data_api_base import (
    DataApiTestParams,
    StandardizedDataApiTestBase,
)
from dl_connector_bundle_chs3.chs3_base.core.us_connection import BaseFileS3Connection
from dl_connector_bundle_chs3_tests.db.base.api.dataset import CHS3DatasetTestBase
from dl_connector_bundle_chs3_tests.db.base.core.base import FILE_CONN_TV
from dl_constants.enums import (
    DataSourceRole,
    FileProcessingStatus,
)
from dl_core.us_manager.us_manager_sync import SyncUSManager


class CHS3DataApiTestBase(CHS3DatasetTestBase[FILE_CONN_TV], StandardizedDataApiTestBase, metaclass=abc.ABCMeta):
    mutation_caches_on = False


class CHS3DataResultTestSuite(
    CHS3DataApiTestBase[FILE_CONN_TV],
    DefaultConnectorDataResultTestSuite,
    metaclass=abc.ABCMeta,
):
    @pytest.mark.xfail(reason="TODO")
    def test_date32(self) -> None:  # TODO implement
        assert 0

    @pytest.mark.parametrize(
        "case, expected_status",
        (
            ("w_status_in_progress", HTTPStatus.BAD_REQUEST),
            ("w_raw_schema_removed", HTTPStatus.OK),  # ok, because raw schema is stored in the dataset
        ),
    )
    def test_file_not_ready_result(
        self,
        case: str,
        expected_status: HTTPStatus,
        sync_us_manager: SyncUSManager,
        data_api: SyncHttpDataApiV2,
        saved_dataset: Dataset,
        saved_connection_id: str,
        sample_file_data_source: BaseFileS3Connection.FileDataSource,
        data_api_test_params: DataApiTestParams,
    ) -> None:
        ds = saved_dataset
        conn = sync_us_manager.get_by_id(saved_connection_id, BaseFileS3Connection)
        conn.update_data_source(
            id=sample_file_data_source.id,
            role=DataSourceRole.origin,
            status=FileProcessingStatus.in_progress if case == "w_status_in_progress" else FileProcessingStatus.ready,
            remove_raw_schema=True if case == "w_raw_schema_removed" else False,
        )
        sync_us_manager.save(conn)

        result_resp = data_api.get_result(
            dataset=ds, fields=[ds.find_field(title=data_api_test_params.date_field)], fail_ok=True
        )
        assert result_resp.status_code == expected_status, result_resp.json

    def test_dataset_with_removed_file(
        self,
        client: FlaskClient,
        control_api: SyncHttpDatasetApiV1,
        data_api: SyncHttpDataApiV2,
        saved_dataset: Dataset,
        saved_connection_id: str,
        data_api_test_params: DataApiTestParams,
    ) -> None:
        ds = saved_dataset
        ds.result_schema["Measure"] = ds.field(formula=f"SUM([{data_api_test_params.summable_field}])")

        ds_resp = control_api.apply_updates(dataset=ds, fail_ok=True)
        assert ds_resp.status_code == HTTPStatus.OK, ds_resp.response_errors
        ds = ds_resp.dataset
        ds = control_api.save_dataset(ds).dataset

        preview_resp = data_api.get_preview(dataset=ds)
        assert preview_resp.status_code == HTTPStatus.OK, preview_resp.json

        # remove source from the connection
        update_resp = client.put(
            "/api/v1/connections/{}".format(saved_connection_id),
            json={"sources": [{"id": "dummy", "title": "dummy", "file_id": "dummy"}]},
        )
        assert update_resp.status_code == HTTPStatus.OK, update_resp.json

        get_ds_resp = client.get(f"/api/v1/datasets/{ds.id}/versions/draft")
        assert get_ds_resp.status_code == HTTPStatus.OK, get_ds_resp.json

        refresh_resp = control_api.refresh_dataset_sources(ds, [ds.sources[0].id], fail_ok=True)
        assert refresh_resp.status_code == HTTPStatus.BAD_REQUEST
        assert refresh_resp.json["code"] == "ERR.DS_API.VALIDATION.ERROR"

        preview_resp = data_api.get_preview(dataset=ds, fail_ok=True)
        assert preview_resp.status_code == HTTPStatus.BAD_REQUEST, preview_resp.json
        assert preview_resp.json["code"] == "ERR.DS_API.DB.SOURCE_DOES_NOT_EXIST"

        result_resp = data_api.get_result(dataset=ds, fields=[ds.result_schema["Measure"]], fail_ok=True)
        assert result_resp.status_code == HTTPStatus.BAD_REQUEST, preview_resp.json
        assert result_resp.json["code"] == "ERR.DS_API.DB.SOURCE_DOES_NOT_EXIST"

    def test_table_name_spoofing(
        self,
        data_api: SyncHttpDataApiV2,
        saved_dataset: Dataset,
        data_api_test_params: DataApiTestParams,
    ) -> None:
        fake_filename = "hack_me.native"
        fake_parameters = {
            "db_name": "fake db_name",
            "db_version": "fake db_version",
            "table_name": fake_filename,
            "origin_source_id": "fake_source_id",
        }

        ds = saved_dataset
        ds.sources[0].parameters = fake_parameters
        result_resp = data_api.get_result(
            dataset=ds, fields=[ds.find_field(title=data_api_test_params.date_field)], fail_ok=True
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json

        preview_resp = data_api.get_preview(dataset=ds, fail_ok=True)
        assert preview_resp.status_code == HTTPStatus.BAD_REQUEST, preview_resp.json
        # ^ because of fake origin_source_id in parameters
