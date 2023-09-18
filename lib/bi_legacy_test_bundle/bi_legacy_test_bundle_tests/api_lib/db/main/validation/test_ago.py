from __future__ import annotations

from http import HTTPStatus

from dl_api_client.dsmaker.primitives import Dataset


def test_ago_errors(api_v1, data_api_v1, dataset_id):
    def check_with_formula(formula: str, formula_check_ok: bool = False) -> None:
        ds = api_v1.load_dataset(dataset=Dataset(id=dataset_id)).dataset
        ds.result_schema["Sales Sum"] = ds.field(formula="SUM([Sales])")
        ds = api_v1.apply_updates(dataset=ds, fail_ok=True).dataset

        check_field = ds.field(formula=formula, title="Ago Field")

        # Validate formula
        resp = api_v1.validate_field(dataset=ds, field=check_field)
        if formula_check_ok:
            assert resp.status_code == HTTPStatus.OK, resp.response_errors
        else:
            assert resp.status_code == HTTPStatus.BAD_REQUEST, resp.response_errors

        # Validate dataset with added field
        ds.result_schema["Ago Field"] = check_field
        ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True)
        assert ds_resp.status_code == HTTPStatus.BAD_REQUEST, ds_resp.response_errors

    # No args
    check_with_formula(formula="AGO()")

    # Single arg - dimension
    check_with_formula(formula="AGO([Category])")

    # Single arg - measure
    check_with_formula(formula="AGO([Sales Sum])")

    # First arg - dimension
    check_with_formula(formula="AGO([Category], [Order Date])")

    # Second arg - measure
    check_with_formula(formula="AGO([Sales Sum], [Sales Sum])")

    # Extra args
    check_with_formula(formula='AGO([Sales Sum], [Order Date], "month", 1, 34)')

    # Nonexistent field in first arg
    check_with_formula(formula='AGO([Nonexistent Field], [Order Date], "month", 1)')

    # Nested nonexistent field in first arg
    check_with_formula(formula='AGO(SUM([Nonexistent Field]), [Order Date], "month", 1)')

    # Nonexistent field in second arg
    check_with_formula(formula='AGO([Sales Sum], [Nonexistent Field], "month", 1)')

    # Nested nonexistent field in second arg
    check_with_formula(formula='AGO([Sales Sum], DATETRUNC([Nonexistent Field], "month"), "month", 1)')

    # Lookup dimension in IGNORE DIMENSIONS
    check_with_formula(formula="AGO([Sales Sum], [Order Date] IGNORE DIMENSIONS [Order Date])")

    # Constant lookup dimension
    check_with_formula(formula="AGO([Sales Sum], #2014-02-02#)")
    check_with_formula(formula="AGO([Sales Sum], TODAY())")


def test_nested_ago_in_compeng(api_v1, data_api_v1, dataset_id):
    ds = api_v1.load_dataset(dataset=Dataset(id=dataset_id)).dataset

    ds.result_schema["Rsum"] = ds.field(formula="RSUM(SUM([Sales]))")
    ds.result_schema["Ago"] = ds.field(formula="AGO([Rsum], [Order Date])")
    ds.result_schema["Nested Ago"] = ds.field(formula="AGO([Ago], [Order Date])")
    ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.OK, ds_resp.response_errors
