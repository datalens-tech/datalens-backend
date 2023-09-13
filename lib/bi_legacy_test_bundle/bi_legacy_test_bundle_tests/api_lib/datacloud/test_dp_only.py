import pytest

from bi_api_client.dsmaker.primitives import Dataset
from bi_testing_ya.dlenv import DLEnv

from bi_connector_clickhouse.core.clickhouse.constants import SOURCE_TYPE_CH_TABLE


@pytest.mark.asyncio
@pytest.mark.parametrize("dl_env", [DLEnv.dc_testing], indirect=True)
async def test_create_dp_app(dl_env, dc_rs_dp_low_level_client):
    resp = await dc_rs_dp_low_level_client.get("/ping")
    assert resp.status == 200


@pytest.mark.parametrize("dl_env", [DLEnv.dc_testing], indirect=True)
def test_simple_dataset(dl_env, dc_rs_ds_api_set, dc_rs_connection_id_clickhouse, dc_rs_workbook_id):
    api_v1 = dc_rs_ds_api_set.control_api_v1
    data_api_v1 = dc_rs_ds_api_set.data_api_v1

    ds = Dataset()
    ds.sources['source_1'] = ds.source(
        source_type=SOURCE_TYPE_CH_TABLE,
        connection_id=dc_rs_connection_id_clickhouse,
        parameters=dict(
            db_name='test_data',
            table_name='sample_superstore',
        )
    )
    ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()
    ds = api_v1.apply_updates(dataset=ds).dataset
    ds_resp = api_v1.save_dataset(
        dataset=ds,
        workbook_id=dc_rs_workbook_id,
    )
    assert ds_resp.status_code == 200
    ds = ds_resp.dataset

    result_reps = data_api_v1.get_result(dataset=ds, fields=[ds.find_field(title="City")])
    assert result_reps.status_code == 200
    city_list = [row[0] for row in result_reps.json['result']['data']['Data']]
    assert 'Baltimore' in city_list
