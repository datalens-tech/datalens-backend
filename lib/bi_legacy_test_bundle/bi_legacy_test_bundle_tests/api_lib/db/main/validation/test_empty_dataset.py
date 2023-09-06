from http import HTTPStatus

from bi_api_client.dsmaker.primitives import Dataset

from bi_legacy_test_bundle_tests.api_lib.utils import data_source_settings_from_table


def test_add_source_to_empty_ds_with_formulas(clickhouse_table, connection_id, api_v1):
    ds = Dataset()
    # Add source
    ds.sources['old_source'] = ds.source(
        connection_id=connection_id, **data_source_settings_from_table(clickhouse_table))
    ds.source_avatars['old_avatar'] = ds.sources['old_source'].avatar()
    ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.OK, ds_resp.response_errors
    ds = ds_resp.dataset
    # Add formula field
    ds.source_avatars['just_a_field'] = ds.field(formula='#2010-01-01#')
    ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.OK, ds_resp.response_errors
    ds = ds_resp.dataset
    ds = api_v1.save_dataset(dataset=ds).dataset
    # Remove source
    ds = api_v1.apply_updates(dataset=ds, fail_ok=True, updates=[
        ds.source_avatars['old_avatar'].delete(),
        ds.sources['old_source'].delete(),
    ]).dataset
    ds = api_v1.save_dataset(dataset=ds).dataset
    assert len(ds.sources) == 0
    # Add new source
    ds.sources['new_source'] = ds.source(
        connection_id=connection_id, **data_source_settings_from_table(clickhouse_table))
    ds.source_avatars['new_avatar'] = ds.sources['new_source'].avatar()
    ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.OK, ds_resp.response_errors
