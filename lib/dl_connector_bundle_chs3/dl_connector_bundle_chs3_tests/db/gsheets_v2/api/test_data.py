import datetime
from http import HTTPStatus

from flask.testing import FlaskClient
import pytest

from dl_api_client.dsmaker.api.data_api import SyncHttpDataApiV2
from dl_api_client.dsmaker.primitives import Dataset
from dl_api_lib_testing.connector.data_api_suites import (
    DefaultConnectorDataDistinctTestSuite,
    DefaultConnectorDataGroupByFormulaTestSuite,
    DefaultConnectorDataPreviewTestSuite,
    DefaultConnectorDataRangeTestSuite,
    DefaultConnectorDataResultTestSuite,
)
from dl_api_lib_testing.data_api_base import DataApiTestParams
from dl_constants.enums import (
    ComponentErrorLevel,
    ComponentType,
    DataSourceRole,
    FileProcessingStatus,
)
from dl_core.us_manager.us_manager_sync import SyncUSManager
from dl_testing.regulated_test import RegulatedTestParams

from dl_connector_bundle_chs3.chs3_gsheets.core.constants import NOTIF_TYPE_GSHEETS_V2_STALE_DATA
from dl_connector_bundle_chs3.chs3_gsheets.core.lifecycle import GSheetsFileS3ConnectionLifecycleManager
from dl_connector_bundle_chs3.chs3_gsheets.core.us_connection import GSheetsFileS3Connection
from dl_connector_bundle_chs3_tests.db.base.api.data import CHS3DataResultTestSuite
from dl_connector_bundle_chs3_tests.db.gsheets_v2.api.base import GSheetsFileS3DataApiTestBase


class TestGSheetsFileS3DataResult(GSheetsFileS3DataApiTestBase, CHS3DataResultTestSuite):
    test_params = RegulatedTestParams(
        mark_features_skipped={
            DefaultConnectorDataResultTestSuite.array_support: "GSheets V2 connector doesn't support arrays",
        },
    )

    @pytest.mark.asyncio
    def test_update_data(
        self,
        sync_us_manager: SyncUSManager,
        saved_connection_id: str,
        data_api: SyncHttpDataApiV2,
        saved_dataset: Dataset,
        data_api_test_params: DataApiTestParams,
        mock_file_uploader_api,
    ) -> None:
        ds = saved_dataset

        # prepare connection sources: set updated time to the current moment
        conn = sync_us_manager.get_by_id(saved_connection_id, GSheetsFileS3Connection)
        dt_now = datetime.datetime.now(datetime.timezone.utc)
        data_updated_at_orig = dt_now
        for src in conn.data.sources:
            src.data_updated_at = dt_now
        sync_us_manager.save(conn)

        def get_notifications_from_result_resp() -> list[dict]:
            result_resp = data_api.get_result(
                dataset=ds, fields=[ds.find_field(title=data_api_test_params.date_field)], fail_ok=True
            )
            return result_resp.json.get("notifications", [])

        # it is not time to update data yet, so we expect no data updates or corresponding notifications
        notifications = get_notifications_from_result_resp()
        assert all(
            notification["locator"] != NOTIF_TYPE_GSHEETS_V2_STALE_DATA.value for notification in notifications
        ), notifications
        conn = sync_us_manager.get_by_id(saved_connection_id, GSheetsFileS3Connection)
        assert conn.data.sources[0].data_updated_at == data_updated_at_orig

        # trigger data update by setting the data update time in the connection to N minutes ago
        data_updated_at = conn.data.oldest_data_update_time() - datetime.timedelta(
            seconds=GSheetsFileS3ConnectionLifecycleManager.STALE_THRESHOLD_SECONDS + 60,  # just in case
        )
        for src in conn.data.sources:
            src.data_updated_at = data_updated_at
        sync_us_manager.save(conn)

        # now notifications should be there, as well as connection sources should be updated
        notifications = get_notifications_from_result_resp()
        assert any(
            notification["locator"] == NOTIF_TYPE_GSHEETS_V2_STALE_DATA.value for notification in notifications
        ), notifications
        conn = sync_us_manager.get_by_id(saved_connection_id, GSheetsFileS3Connection)
        assert conn.data.sources[0].data_updated_at != data_updated_at

    def test_component_error(
        self,
        sync_us_manager: SyncUSManager,
        saved_connection_id: str,
        saved_dataset: Dataset,
        client: FlaskClient,
        data_api: SyncHttpDataApiV2,
        data_api_test_params: DataApiTestParams,
    ) -> None:
        conn = sync_us_manager.get_by_id(saved_connection_id, GSheetsFileS3Connection)
        err_details = {"error": "details", "request-id": "637"}
        conn.data.component_errors.add_error(
            id=conn.data.sources[0].id,
            type=ComponentType.data_source,
            message="Custom error message",
            code=["FILE", "CUSTOM_FILE_ERROR"],
            details=err_details,
        )
        conn.update_data_source(
            conn.data.sources[0].id,
            role=DataSourceRole.origin,
            s3_filename=None,
            status=FileProcessingStatus.failed,
            preview_id=None,
            data_updated_at=datetime.datetime.now(datetime.timezone.utc),
        )
        sync_us_manager.save(conn)

        ds = saved_dataset
        result_resp = data_api.get_result(
            dataset=ds, fields=[ds.find_field(title=data_api_test_params.distinct_field)], fail_ok=True
        )
        assert result_resp.status_code == HTTPStatus.BAD_REQUEST, result_resp.json
        assert result_resp.json["details"] == err_details
        assert result_resp.json["message"] == "Custom error message"
        assert result_resp.json["code"] == "ERR.DS_API.SOURCE.FILE.CUSTOM_FILE_ERROR"

        conn_resp = client.get(f"/api/v1/connections/{saved_connection_id}")
        assert conn_resp.status_code == HTTPStatus.OK, conn_resp.json
        assert conn_resp.json["component_errors"], conn_resp.json
        actual_errors = conn_resp.json["component_errors"]["items"][0]["errors"]
        assert len(actual_errors) == 1, actual_errors
        assert actual_errors[0]["code"] == "ERR.DS_API.SOURCE.FILE.CUSTOM_FILE_ERROR"

    @pytest.mark.asyncio
    def test_component_error_warning(
        self,
        sync_us_manager: SyncUSManager,
        saved_connection_id: str,
        saved_dataset: Dataset,
        data_api: SyncHttpDataApiV2,
        data_api_test_params: DataApiTestParams,
        mock_file_uploader_api,
    ) -> None:
        conn = sync_us_manager.get_by_id(saved_connection_id, GSheetsFileS3Connection)

        long_long_ago = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
            seconds=GSheetsFileS3ConnectionLifecycleManager.STALE_THRESHOLD_SECONDS + 60,  # just in case
        )
        err_details = {"error": "details", "request-id": "637"}
        conn.data.component_errors.add_error(
            id=conn.data.sources[0].id,
            type=ComponentType.data_source,
            message="Custom error message",
            code=["FILE", "CUSTOM_FILE_ERROR"],
            details=err_details,
            level=ComponentErrorLevel.warning,
        )
        conn.update_data_source(
            conn.data.sources[0].id,
            role=DataSourceRole.origin,
            data_updated_at=long_long_ago,
        )
        sync_us_manager.save(conn)

        ds = saved_dataset
        result_resp = data_api.get_result(
            dataset=ds, fields=[ds.find_field(title=data_api_test_params.distinct_field)], fail_ok=True
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        assert len(result_resp.json["notifications"]) == 2
        assert "Reason: FILE.CUSTOM_FILE_ERROR, Request-ID: 637" in result_resp.json["notifications"][0]["message"]
        conn = sync_us_manager.get_by_id(saved_connection_id, GSheetsFileS3Connection)
        assert conn.data.sources[0].data_updated_at > long_long_ago  # data update was triggered


class TestGSheetsFileS3DataGroupBy(GSheetsFileS3DataApiTestBase, DefaultConnectorDataGroupByFormulaTestSuite):
    pass


class TestGSheetsFileS3DataRange(GSheetsFileS3DataApiTestBase, DefaultConnectorDataRangeTestSuite):
    pass


class TestGSheetsFileDataDistinct(GSheetsFileS3DataApiTestBase, DefaultConnectorDataDistinctTestSuite):
    test_params = RegulatedTestParams(
        mark_tests_failed={
            DefaultConnectorDataDistinctTestSuite.test_date_filter_distinct: "FIXME",
        }
    )


class TestGSheetsFileS3DataPreview(GSheetsFileS3DataApiTestBase, DefaultConnectorDataPreviewTestSuite):
    pass
