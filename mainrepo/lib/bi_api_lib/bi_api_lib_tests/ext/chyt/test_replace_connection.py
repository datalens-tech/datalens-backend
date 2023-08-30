from __future__ import annotations

from bi_constants.enums import CreateDSFrom

from bi_api_client.dsmaker.primitives import Dataset

from bi_api_lib_tests.utils import replace_dataset_connection as replace_connection


def _test_chyt_to_chyt(
        api_v1, client,
        conn_id, new_conn_id,
        create_ds_from,
        expected_source_type
):
    src_kwargs = dict(
        source_type=create_ds_from,
        parameters=dict(
            subsql='select 1 as value',
        ),
    )

    ds = Dataset()
    ds.sources['source_1'] = ds.source(
        connection_id=conn_id,
        **src_kwargs)
    ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()
    ds = api_v1.apply_updates(dataset=ds).dataset
    ds_resp = api_v1.save_dataset(dataset=ds, preview=False)
    assert ds_resp.status_code == 200
    ds = ds_resp.dataset

    # original_result_schema = ds.result_schema
    ds = replace_connection(
        api_v1, ds=ds,
        old_connection_id=conn_id, new_connection_id=new_conn_id,
    ).dataset
    assert ds.sources[0].source_type == expected_source_type

    ds = api_v1.save_dataset(dataset=ds).dataset

    client.delete('/api/v1/datasets/{}'.format(ds.id))


def test_chyt_to_chyt(
    api_v1, client, public_ch_over_yt_connection_id, public_ch_over_yt_second_connection_id,
    public_ch_over_yt_user_auth_headers
):
    _test_chyt_to_chyt(
        api_v1, client,
        public_ch_over_yt_connection_id, public_ch_over_yt_second_connection_id,
        CreateDSFrom.CHYT_SUBSELECT,
        expected_source_type=CreateDSFrom.CHYT_SUBSELECT
    )


def test_chyt_user_auth_to_chyt(
    api_v1_with_token, client, public_ch_over_yt_user_auth_connection_id, public_ch_over_yt_connection_id,
    public_ch_over_yt_user_auth_headers
):
    _test_chyt_to_chyt(
        api_v1_with_token, client,
        public_ch_over_yt_user_auth_connection_id, public_ch_over_yt_connection_id,
        CreateDSFrom.CHYT_USER_AUTH_SUBSELECT,
        expected_source_type=CreateDSFrom.CHYT_SUBSELECT
    )
