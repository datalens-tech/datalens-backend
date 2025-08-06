import http

import pytest

from dl_api_client.dsmaker.api.dataset_api import SyncHttpDatasetApiV1
from dl_api_client.dsmaker.primitives import Dataset
from dl_api_lib_tests.db.base import DefaultApiTestBase
from dl_core.constants import DatasetConstraints


class TestUISettings(DefaultApiTestBase):
    def test_add_field_with_ui_settings(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
    ) -> None:
        ui_settings = '{"color": "blue", "visible": true, "position": {"x": 10, "y": 20}}'
        a_field = saved_dataset.result_schema[0]

        ds_resp = control_api.apply_updates(
            dataset=saved_dataset,
            updates=[
                {
                    "action": "add_field",
                    "field": {
                        "guid": "test_ui_settings_field",
                        "title": "Copy {}".format(a_field.title),
                        "calc_mode": "formula",
                        "formula": "[{}]".format(a_field.title),
                        "ui_settings": ui_settings,
                    },
                }
            ],
            fail_ok=True,
        )

        assert ds_resp.status_code == http.HTTPStatus.OK

        field = ds_resp.dataset.find_field("Copy {}".format(a_field.title))
        assert field.id == "test_ui_settings_field"
        assert field.ui_settings == ui_settings

    @pytest.mark.parametrize(
        "data_value,data_size",
        [
            ("x", DatasetConstraints.FIELD_UI_SETTINGS_MAX_SIZE),
            ("я", int(DatasetConstraints.FIELD_UI_SETTINGS_MAX_SIZE / 2)),
        ],
    )
    def test_field_with_ui_settings_limit(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
        data_value: str,
        data_size: int,
    ) -> None:
        oversized_ui_settings = '{"data": "' + data_value * data_size + '"}'
        a_field = saved_dataset.result_schema[0]

        ds_resp = control_api.apply_updates(
            dataset=saved_dataset,
            updates=[
                {
                    "action": "add_field",
                    "field": {
                        "title": "Copy {}".format(a_field.title),
                        "calc_mode": "formula",
                        "formula": "[{}]".format(a_field.title),
                        "ui_settings": oversized_ui_settings,
                    },
                }
            ],
            fail_ok=True,
        )

        assert ds_resp.status_code == http.HTTPStatus.BAD_REQUEST
        assert ds_resp.bi_status_code == "ERR.DS_API.VALIDATION.FATAL"

    def test_field_with_ui_settings_overall_limit(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
    ) -> None:
        large_ui_settings = '{"data": "' + "x" * (DatasetConstraints.FIELD_UI_SETTINGS_MAX_SIZE - 20) + '"}'
        a_field = saved_dataset.result_schema[0]
        ds = saved_dataset

        limit = int(DatasetConstraints.OVERALL_UI_SETTINGS_MAX_SIZE // DatasetConstraints.FIELD_UI_SETTINGS_MAX_SIZE)

        for i in range(limit):
            ds_resp = control_api.apply_updates(
                dataset=ds,
                updates=[
                    {
                        "action": "add_field",
                        "field": {
                            "title": "Copy {} {}".format(i, a_field.title),
                            "calc_mode": "formula",
                            "formula": "[{}]".format(a_field.title),
                            "ui_settings": large_ui_settings,
                        },
                    }
                ],
                fail_ok=True,
            )

            assert ds_resp.status_code == http.HTTPStatus.OK
            ds = ds_resp.dataset

        ds_resp = control_api.apply_updates(
            dataset=ds_resp.dataset,
            updates=[
                {
                    "action": "add_field",
                    "field": {
                        "title": "One more copy {}".format(a_field.title),
                        "calc_mode": "formula",
                        "formula": "[{}]".format(a_field.title),
                        "ui_settings": large_ui_settings,
                    },
                }
            ],
            fail_ok=True,
        )

        assert ds_resp.status_code == http.HTTPStatus.BAD_REQUEST
        assert ds_resp.bi_status_code == "ERR.DS_API.VALIDATION.FATAL"

    def test_update_field_with_ui_settings(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
    ) -> None:
        initial_ui_settings = '{"color": "blue", "visible": true}'
        a_field = saved_dataset.result_schema[0]

        ds_resp = control_api.apply_updates(
            dataset=saved_dataset,
            updates=[
                {
                    "action": "add_field",
                    "field": {
                        "guid": "test_update_ui_settings",
                        "title": "Copy {}".format(a_field.title),
                        "calc_mode": "formula",
                        "formula": "[{}]".format(a_field.title),
                        "ui_settings": initial_ui_settings,
                    },
                }
            ],
            fail_ok=True,
        )

        assert ds_resp.status_code == http.HTTPStatus.OK

        updated_ui_settings = '{"color": "red", "visible": false, "position": {"x": 15, "y": 25}}'

        ds_resp = control_api.apply_updates(
            dataset=ds_resp.dataset,
            updates=[
                {
                    "action": "update_field",
                    "field": {
                        "guid": "test_update_ui_settings",
                        "ui_settings": updated_ui_settings,
                    },
                }
            ],
            fail_ok=True,
        )

        assert ds_resp.status_code == http.HTTPStatus.OK

        field = ds_resp.dataset.find_field("Copy {}".format(a_field.title))
        assert field.ui_settings == updated_ui_settings

    def test_remove_field_ui_settings(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
    ) -> None:
        initial_ui_settings = '{"color": "green", "visible": true}'
        a_field = saved_dataset.result_schema[0]

        ds_resp = control_api.apply_updates(
            dataset=saved_dataset,
            updates=[
                {
                    "action": "add_field",
                    "field": {
                        "guid": "test_remove_ui_settings",
                        "title": "Copy {}".format(a_field.title),
                        "calc_mode": "formula",
                        "formula": "[{}]".format(a_field.title),
                        "ui_settings": initial_ui_settings,
                    },
                }
            ],
            fail_ok=True,
        )

        assert ds_resp.status_code == http.HTTPStatus.OK

        ds_resp = control_api.apply_updates(
            dataset=ds_resp.dataset,
            updates=[
                {
                    "action": "update_field",
                    "field": {
                        "guid": "test_remove_ui_settings",
                        "ui_settings": "",
                    },
                }
            ],
            fail_ok=True,
        )

        assert ds_resp.status_code == http.HTTPStatus.OK

        field = ds_resp.dataset.find_field("Copy {}".format(a_field.title))
        assert field.ui_settings == ""

    def test_update_field_exceeding_ui_settings_limit(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
    ) -> None:
        initial_ui_settings = '{"color": "purple", "visible": true}'
        a_field = saved_dataset.result_schema[0]

        ds_resp = control_api.apply_updates(
            dataset=saved_dataset,
            updates=[
                {
                    "action": "add_field",
                    "field": {
                        "guid": "test_update_exceed_limit",
                        "title": "Copy {}".format(a_field.title),
                        "calc_mode": "formula",
                        "formula": "[{}]".format(a_field.title),
                        "ui_settings": initial_ui_settings,
                    },
                }
            ],
            fail_ok=True,
        )

        assert ds_resp.status_code == http.HTTPStatus.OK, ds_resp.response_errors

        oversized_ui_settings = '{"data": "' + "x" * DatasetConstraints.FIELD_UI_SETTINGS_MAX_SIZE + '"}'

        ds_resp = control_api.apply_updates(
            dataset=ds_resp.dataset,
            updates=[
                {
                    "action": "update_field",
                    "field": {
                        "guid": "test_update_exceed_limit",
                        "ui_settings": oversized_ui_settings,
                    },
                }
            ],
            fail_ok=True,
        )

        assert ds_resp.status_code == http.HTTPStatus.BAD_REQUEST
        assert ds_resp.bi_status_code == "ERR.DS_API.VALIDATION.FATAL"
