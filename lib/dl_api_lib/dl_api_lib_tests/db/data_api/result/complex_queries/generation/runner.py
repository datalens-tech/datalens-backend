from __future__ import annotations

from http import HTTPStatus
import json
from typing import Optional

import attr
import pytest

from dl_api_client.dsmaker.api.data_api import SyncHttpDataApiV2
from dl_api_client.dsmaker.api.dataset_api import SyncHttpDatasetApiV1
from dl_api_client.dsmaker.primitives import Dataset
from dl_api_client.dsmaker.shortcuts.result_data import get_data_rows
from dl_api_lib_tests.db.data_api.result.complex_queries.generation.generator import TestSettings


class Error400(Exception):
    pass


@attr.s
class PreGeneratedLODTestRunner:
    dataset_id: str = attr.ib(kw_only=True)
    control_api: SyncHttpDatasetApiV1 = attr.ib(kw_only=True)
    data_api: SyncHttpDataApiV2 = attr.ib(kw_only=True)

    def get_measure_data(
        self,
        ds: Dataset,
        test_settings: TestSettings,
        measures: list[str],
    ) -> dict[str, list[float]]:
        result_resp = self.data_api.get_result(
            dataset=ds,
            fields=[
                *[ds.find_field(title=name) for name in test_settings.base_dimensions],
                *[ds.find_field(title=name) for name in measures],
            ],
            filters=[
                ds.find_field(title=name).filter(op, values) for name, (op, values) in test_settings.filters.items()
            ],
            order_by=[
                *[ds.find_field(title=name) for name in test_settings.base_dimensions],
            ],
            fail_ok=True,
        )

        if result_resp.status_code == HTTPStatus.BAD_REQUEST:
            raise Error400(result_resp.json)

        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)
        assert data_rows

        def float_or_none(value: Optional[str]) -> Optional[float]:
            if value is None:
                return None
            return float(value)

        result: dict[str, list[float]] = {}
        dim_cnt = len(test_settings.base_dimensions)
        for measure_idx, name in enumerate(measures):
            result[name] = [float_or_none(row[measure_idx + dim_cnt]) for row in data_rows]

        return result

    def run_test(self, test_settings: TestSettings, ignore_400_error: bool = False) -> None:
        ds = self.control_api.load_dataset(Dataset(id=self.dataset_id)).dataset
        measure_fields = [
            (f"Measure {measure_idx}", measure_formula)
            for measure_idx, measure_formula in enumerate(test_settings.measure_formulas)
        ]
        for measure_name, measure_formula in measure_fields:
            ds.result_schema[measure_name] = ds.field(formula=measure_formula)

        try:
            all_measures = self.get_measure_data(
                ds=ds, test_settings=test_settings, measures=[name for name, _ in measure_fields]
            )
            for measure_name, _measure_formula in measure_fields:
                one_measure = self.get_measure_data(ds=ds, test_settings=test_settings, measures=[measure_name])
                assert pytest.approx(all_measures[measure_name]) == one_measure[measure_name]
        except Error400:
            if not ignore_400_error:
                raise
            print("Ignoring Error400")

    def run_test_list(self, test_list: list[TestSettings], ignore_400_error: bool) -> None:
        for test_idx, test_settings in enumerate(test_list):
            print(f"\nTest # {test_idx}\n{json.dumps(test_settings.serialize())}\n")
            self.run_test(test_settings, ignore_400_error=ignore_400_error)
