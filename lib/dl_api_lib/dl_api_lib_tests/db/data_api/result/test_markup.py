from __future__ import annotations

import uuid

import pytest

from dl_api_client.dsmaker.shortcuts.result_data import get_data_rows
from dl_api_lib_testing.data_api_base import DataApiTestParams
from dl_api_lib_tests.db.base import DefaultApiTestBase


class TestUMarkup(DefaultApiTestBase):
    @pytest.fixture(scope="function")
    def data_api_test_params(self, sample_table) -> DataApiTestParams:
        # This default is defined for the sample table
        return DataApiTestParams(
            two_dims=("category", "city"),
            summable_field="sales",
            range_field="sales",
            distinct_field="city",
            date_field="order_date",
        )

    def test_markup(self, saved_dataset, data_api, data_api_test_params):
        ds = saved_dataset

        field_a, field_b, field_c, field_d, field_e, field_nulled = (str(uuid.uuid4()) for _ in range(6))
        formula_a = """
            markup(
                italic(url(
                    "http://example.com/?city=" + [city] + "&_=1",
                    [city])),
                " (", bold(str([order_date])), ")")
        """
        formula_b = """
            url("https://example.com/?text=" + str([sales]) + " usd в рублях", str([sales]))
        """
        formula_c = """
                    image([city], [postal_code], 13, "alt_text")
                """
        formula_d = """
                    image([city], [postal_code], NULL, NULL)
                """
        formula_e = """
                    image([city])
                """
        formula_nulled = """
            url(if([sales] > 0, NULL, "neg"), str([sales]))
        """

        result_resp = data_api.get_result(
            dataset=ds,
            updates=[
                ds.field(
                    id=field_a,
                    title="Field A",
                    formula=formula_a,
                ).add(),
                ds.field(
                    id=field_b,
                    title="Field B",
                    formula=formula_b,
                ).add(),
                ds.field(
                    id=field_c,
                    title="Field C",
                    formula=formula_c,
                ).add(),
                ds.field(
                    id=field_d,
                    title="Field D",
                    formula=formula_d,
                ).add(),
                ds.field(
                    id=field_e,
                    title="Field E",
                    formula=formula_e,
                ).add(),
                ds.field(
                    id=field_nulled,
                    title="Field Nulled",
                    formula=formula_nulled,
                ).add(),
            ],
            fields=[
                ds.field(id=field_a),
                ds.field(id=field_b),
                ds.field(id=field_c),
                ds.field(id=field_d),
                ds.field(id=field_e),
                ds.field(id=field_nulled),
            ],
        )

        assert result_resp.status_code == 200, result_resp.response_errors
        data_rows = get_data_rows(result_resp)
        assert data_rows

        some_row = data_rows[0]
        assert len(some_row) == 6
        res_a, res_b, res_c, res_d, res_e, res_nulled = some_row
        assert isinstance(res_a, dict)
        assert isinstance(res_b, dict)
        assert isinstance(res_c, dict)
        assert isinstance(res_d, dict)
        assert isinstance(res_e, dict)
        assert res_nulled is None

        b_val = res_b["content"]["content"]
        assert isinstance(b_val, str)
        expected_b = {
            "type": "url",
            "url": f"https://example.com/?text={b_val} usd в рублях",
            "content": {"type": "text", "content": b_val},
        }
        assert res_b == expected_b

        c_height = res_c["height"]
        assert isinstance(c_height, int)
        expected_c = {"type": "img", "src": "Richmond", "width": 23223, "height": 13, "alt": "alt_text"}
        assert res_c == expected_c

        d_alt = res_d["alt"]
        assert d_alt is None
        expected_d = {"type": "img", "src": "Richmond", "width": 23223, "height": None, "alt": None}
        assert res_d == expected_d

        expected_e = {"type": "img", "src": "Richmond", "width": None, "height": None, "alt": None}
        assert res_e == expected_e
