from __future__ import annotations

import time
from http import HTTPStatus

from bi_api_client.dsmaker.primitives import Dataset


def test_dataset_with_many_formula_fields(api_v1, dataset_id):
    ds = api_v1.load_dataset(dataset=Dataset(id=dataset_id)).dataset

    formula_field_cnt = 20
    fields_in_formula = 10

    names = [f.title for f in ds.result_schema]
    for cnt in range(formula_field_cnt):
        new_name = f'Autogen {cnt}'
        ds.result_schema[new_name] = ds.field(
            formula='CONCAT({})'.format(', '.join([
                f'[{name}]' for name in names[cnt:cnt+fields_in_formula]
            ]))
        )
        names.append(new_name)

    ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.OK, ds_resp.response_errors
    ds = ds_resp.dataset

    started = time.monotonic()
    ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True, updates=[
        ds.sources[0].refresh(),
    ])
    finished = time.monotonic()

    assert ds_resp.status_code == HTTPStatus.OK, ds_resp.response_errors
    ds = ds_resp.dataset
    assert len(ds.component_errors.items) == 0
    assert finished - started < 20
