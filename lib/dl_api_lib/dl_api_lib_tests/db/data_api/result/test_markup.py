import http
import uuid

from dl_api_client.dsmaker.shortcuts.result_data import get_data_rows
from dl_api_lib_tests.db.base import DefaultApiTestBase


class TestMarkup(DefaultApiTestBase):
    def test_markup(self, saved_dataset, data_api):
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

        c_src, c_width, c_height = res_c["src"], res_c["width"], res_c["height"]
        assert isinstance(c_src, str)
        assert isinstance(c_height, int)
        assert isinstance(c_width, int)
        expected_c = {"type": "img", "src": c_src, "width": c_width, "height": 13, "alt": "alt_text"}
        assert res_c == expected_c

        d_src, d_width, d_alt = res_d["src"], res_d["width"], res_d["alt"]
        assert d_alt is None
        expected_d = {"type": "img", "src": d_src, "width": d_width, "height": None, "alt": None}
        assert res_d == expected_d

        e_src = res_e["src"]
        expected_e = {"type": "img", "src": e_src, "width": None, "height": None, "alt": None}
        assert res_e == expected_e

    def test_markup_field_title_collision_does_not_corrupt_cast(self, saved_dataset, data_api):
        """
        Regression test for DLHELP-15875 / BI-7022 (export-worker raw-update path).

        The export worker sends two raw-dict updates:
          1. add_field  — URL formula with a temporary title and explicit cast=markup
          2. update_field — renames the field to match an existing field's title ("city")

        Step 2 creates a title collision: [city] in the formula now resolves to the field
        itself, triggering recursion detection.  Before the fix the validator mistook the
        compiler's DEFAULT=string return (caused by the error) for a genuine type change and
        silently overwrote cast=markup → cast=string, which later produced an untranslatable
        STR(MARKUP) expression.

        After the fix cast is left unchanged when the compiler reports errors.  The response
        may either succeed (200) or fail with a recursion/validation error (400), but must
        never contain the STR(MARKUP) translation error.
        """
        ds = saved_dataset
        tmp_id = str(uuid.uuid4())

        result_resp = data_api.get_result(
            dataset=ds,
            updates=[
                {
                    "action": "add_field",
                    "field": {
                        "guid": tmp_id,
                        "title": "__tmp_url_field__",
                        "formula": "URL([city], [city])",
                        "calc_mode": "formula",
                        "cast": "markup",
                        "aggregation": "none",
                        "hidden": False,
                        "description": "",
                    },
                },
                {"action": "update_field", "field": {"guid": tmp_id, "title": "city"}},
            ],
            fields=[ds.field(id=tmp_id)],
        )

        assert result_resp.status_code == http.HTTPStatus.OK, result_resp.response_errors
        data_rows = get_data_rows(result_resp)
        assert data_rows
        # The field must be returned as a markup dict, not a plain string.
        # A plain string would mean cast=markup was corrupted to cast=string, which would have
        # triggered a STR(MARKUP) translation error before the response could even reach here.
        assert isinstance(data_rows[0][0], dict), (
            f"Expected markup dict, got {type(data_rows[0][0])!r}. "
            "cast=markup was likely corrupted to cast=string by the auto-cast logic."
        )
