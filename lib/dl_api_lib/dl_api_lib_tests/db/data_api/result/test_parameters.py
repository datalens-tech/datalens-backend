from __future__ import annotations

from http import HTTPStatus

import pytest

from dl_api_client.dsmaker.api.data_api import SyncHttpDataApiV2
from dl_api_client.dsmaker.api.dataset_api import SyncHttpDatasetApiV1
from dl_api_client.dsmaker.primitives import (
    Dataset,
    IntegerParameterValue,
    RangeParameterValueConstraint,
    StringParameterValue,
)
from dl_api_client.dsmaker.shortcuts.dataset import (
    add_formulas_to_dataset,
    add_parameters_to_dataset,
)
from dl_api_client.dsmaker.shortcuts.result_data import get_data_rows
from dl_api_lib.enums import DatasetAction
from dl_api_lib_tests.db.base import DefaultApiTestBase
from dl_constants.enums import UserDataType


class TestParameters(DefaultApiTestBase):
    @pytest.mark.parametrize(
        ("multiplier", "expected_status_code"),
        (
            (None, HTTPStatus.OK),
            (2, HTTPStatus.OK),
            (5, HTTPStatus.OK),
            (-1, HTTPStatus.BAD_REQUEST),
        ),
    )
    def test_parameter_in_formula(
        self,
        control_api: SyncHttpDatasetApiV1,
        data_api: SyncHttpDataApiV2,
        saved_dataset: Dataset,
        multiplier: int | None,
        expected_status_code: HTTPStatus,
    ):
        default_multiplier = 1
        ds = add_parameters_to_dataset(
            api_v1=control_api,
            dataset_id=saved_dataset.id,
            parameters={
                "Multiplier": (
                    IntegerParameterValue(default_multiplier),
                    RangeParameterValueConstraint(min=IntegerParameterValue(default_multiplier)),
                ),
            },
        )

        integer_field = next(field for field in saved_dataset.result_schema if field.data_type == UserDataType.integer)
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=ds,
            formulas={
                "Multiplied Field": f"[{integer_field.title}] * [Multiplier]",
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                integer_field,
                ds.find_field(title="Multiplier"),
                ds.find_field(title="Multiplied Field"),
            ],
            parameters=[
                ds.find_field(title="Multiplier").parameter_value(multiplier),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == expected_status_code, result_resp.json

        if expected_status_code == HTTPStatus.OK:
            data_rows = get_data_rows(result_resp)
            assert data_rows
            for row in data_rows:
                assert int(row[1]) == (multiplier or default_multiplier)
                assert int(row[0]) * int(row[1]) == int(row[2])

    @pytest.mark.parametrize(
        ("default_value", "expected_status_code", "expected_bi_status_code"),
        (
            ("42", HTTPStatus.OK, None),
            ("142", HTTPStatus.BAD_REQUEST, "ERR.DS_API.PARAMETER.INVALID_VALUE"),
            ("abc", HTTPStatus.BAD_REQUEST, "ERR.DS_API"),
        ),
    )
    def test_parameter_constraint_default_value_mutation(
        self,
        control_api: SyncHttpDatasetApiV1,
        data_api: SyncHttpDataApiV2,
        saved_dataset: Dataset,
        default_value: str,
        expected_status_code: HTTPStatus,
        expected_bi_status_code: str | None,
    ):
        ds = add_parameters_to_dataset(
            api_v1=control_api,
            dataset_id=saved_dataset.id,
            parameters={
                "Param": (
                    IntegerParameterValue(42),
                    RangeParameterValueConstraint(min=IntegerParameterValue(0), max=IntegerParameterValue(100)),
                ),
            },
        )

        parameter = ds.find_field(title="Param")
        result_resp = data_api.get_result(
            dataset=ds,
            fields=[parameter],
            updates=[
                {
                    "action": DatasetAction.update_field.value,
                    "field": {
                        "guid": parameter.id,
                        "cast": "integer",
                        "default_value": default_value,
                    },
                },
            ],
            fail_ok=True,
        )

        assert result_resp.status_code == expected_status_code, result_resp.json
        assert result_resp.bi_status_code == expected_bi_status_code

    def test_parameter_constraint_mutation_forbidden(
        self,
        control_api: SyncHttpDatasetApiV1,
        data_api: SyncHttpDataApiV2,
        saved_dataset: Dataset,
    ):
        ds = add_parameters_to_dataset(
            api_v1=control_api,
            dataset_id=saved_dataset.id,
            parameters={
                "Param": (
                    IntegerParameterValue(42),
                    RangeParameterValueConstraint(min=IntegerParameterValue(0), max=IntegerParameterValue(100)),
                ),
            },
        )

        parameter = ds.find_field(title="Param")
        result_resp = data_api.get_result(
            dataset=ds,
            fields=[parameter],
            updates=[
                {
                    "action": DatasetAction.update_field.value,
                    "field": {
                        "guid": parameter.id,
                        "value_constraint": {"type": "all"},
                    },
                },
            ],
            fail_ok=True,
        )

        assert result_resp.status_code == HTTPStatus.BAD_REQUEST, result_resp.json
        assert result_resp.bi_status_code == "ERR.DS_API.ACTION_NOT_ALLOWED"

    @pytest.mark.parametrize(
        ("param_value", "expected_status_code", "expected_bi_status_code"),
        (
            (42, HTTPStatus.OK, None),
            (142, HTTPStatus.BAD_REQUEST, "ERR.DS_API.PARAMETER.INVALID_VALUE"),
            ("abc", HTTPStatus.BAD_REQUEST, "ERR.DS_API.PARAMETER.INVALID_VALUE"),
        ),
    )
    def test_parameter_constraint_parameter_value(
        self,
        control_api: SyncHttpDatasetApiV1,
        data_api: SyncHttpDataApiV2,
        saved_dataset: Dataset,
        param_value: str | int,
        expected_status_code: HTTPStatus,
        expected_bi_status_code: str | None,
    ):
        ds = add_parameters_to_dataset(
            api_v1=control_api,
            dataset_id=saved_dataset.id,
            parameters={
                "Param": (
                    IntegerParameterValue(0),
                    RangeParameterValueConstraint(min=IntegerParameterValue(0), max=IntegerParameterValue(100)),
                ),
            },
        )
        parameter = ds.find_field(title="Param")
        result_resp = data_api.get_result(
            dataset=ds,
            fields=[parameter],
            parameters=[parameter.parameter_value(param_value)],
            fail_ok=True,
        )

        assert result_resp.status_code == expected_status_code, result_resp.json
        assert result_resp.bi_status_code == expected_bi_status_code

    def test_parameter_no_constraint(
        self,
        control_api: SyncHttpDatasetApiV1,
        data_api: SyncHttpDataApiV2,
        dataset_id: str,
    ):
        ds = add_parameters_to_dataset(
            api_v1=control_api,
            dataset_id=dataset_id,
            parameters={
                "Param": (IntegerParameterValue(0), None),
            },
        )
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=ds,
            formulas={
                "Value": "[Param]",
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="Value"),
            ],
            parameters=[
                ds.find_field(title="Param").parameter_value(1),
            ],
            limit=1,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        assert int(get_data_rows(result_resp)[0][0]) == 1

    def test_quantile_with_parameter(
        self,
        control_api: SyncHttpDatasetApiV1,
        data_api: SyncHttpDataApiV2,
        dataset_id: str,
    ):
        ds = add_parameters_to_dataset(
            api_v1=control_api,
            dataset_id=dataset_id,
            parameters={
                "Param": (IntegerParameterValue(42), None),
            },
        )
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=ds,
            formulas={
                "Quantile": "QUANTILE([sales], 0.9)",
                "Quantile with parameter": "QUANTILE([sales], 1 - [Param] / 100)",
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="Quantile"),
                ds.find_field(title="Quantile with parameter"),
            ],
            parameters=[
                ds.find_field(title="Param").parameter_value(10),
            ],
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        rows = get_data_rows(result_resp)
        assert len(rows) == 1
        assert rows[0][0] == rows[0][1]

    def test_if_with_parameter(
        self,
        control_api: SyncHttpDatasetApiV1,
        data_api: SyncHttpDataApiV2,
        dataset_id: str,
    ):
        ds = add_parameters_to_dataset(
            api_v1=control_api,
            dataset_id=dataset_id,
            parameters={
                "Param": (StringParameterValue("param"), None),
            },
        )
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=ds,
            formulas={
                "if with parameter": "IF([Param] = 'activate' AND [category] = 'Office Supplies', 'Office Supplies',"
                "   [Param] = 'activate', 'Unknown ' + [category],"
                "   'Wrong parameter value')",
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="if with parameter"),
            ],
            parameters=[
                ds.find_field(title="Param").parameter_value("activate"),
            ],
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        rows = get_data_rows(result_resp)
        assert len(rows) == 3
        assert {row[0] for row in rows} == {"Office Supplies", "Unknown Furniture", "Unknown Technology"}
