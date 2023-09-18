from __future__ import annotations

from http import HTTPStatus

from bi_legacy_test_bundle_tests.api_lib.utils import make_dataset_for_replacing as make_dataset
from bi_legacy_test_bundle_tests.api_lib.utils import replace_dataset_connection as replace_connection

from bi_connector_metrica.core.constants import (
    SOURCE_TYPE_APPMETRICA_API,
    SOURCE_TYPE_METRICA_API,
)


def test_ch_to_metrika_api(
    api_v1,
    clickhouse_db,
    static_connection_id,
    metrika_api_connection_id,
    appmetrica_api_connection_id,
):
    old_connection_id, new_connection_id = static_connection_id, metrika_api_connection_id
    old_db = clickhouse_db

    ds = make_dataset(api_v1, old_connection_id=old_connection_id, old_db=old_db, multitable=False)

    original_result_schema = ds.result_schema
    ds = replace_connection(
        api_v1, ds=ds, old_connection_id=old_connection_id, new_connection_id=new_connection_id, fail_ok=True
    ).dataset

    assert ds.sources[0].source_type == SOURCE_TYPE_METRICA_API

    ds_resp = api_v1.apply_updates(dataset=ds, updates=[field.delete() for field in original_result_schema])
    assert ds_resp.status_code == HTTPStatus.OK
    ds = ds_resp.dataset

    # replace it again (to appmetrica)
    original_result_schema = ds.result_schema
    ds = replace_connection(
        api_v1, ds=ds, old_connection_id=new_connection_id, new_connection_id=appmetrica_api_connection_id, fail_ok=True
    ).dataset
    assert ds.sources[0].source_type == SOURCE_TYPE_APPMETRICA_API

    ds_resp = api_v1.apply_updates(dataset=ds, updates=[field.delete() for field in original_result_schema])
    assert ds_resp.status_code == HTTPStatus.OK
    ds = ds_resp.dataset

    api_v1.save_dataset(dataset=ds)
