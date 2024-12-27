from __future__ import annotations

from http import HTTPStatus

import pytest

from dl_api_client.dsmaker.primitives import (
    IntegerParameterValue,
    RangeParameterValueConstraint,
    StringParameterValue,
)
from dl_api_client.dsmaker.shortcuts.dataset import (
    add_formulas_to_dataset,
    add_parameters_to_dataset,
)
from dl_api_client.dsmaker.shortcuts.result_data import get_data_rows
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
    def test_parameter_in_formula(self, control_api, data_api, saved_dataset, multiplier, expected_status_code):
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

    def test_parameter_no_constraint(self, control_api, data_api, dataset_id):
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

    def test_quantile_with_parameter(self, control_api, data_api, dataset_id):
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

    def test_if_with_parameter(self, control_api, data_api, dataset_id):
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
