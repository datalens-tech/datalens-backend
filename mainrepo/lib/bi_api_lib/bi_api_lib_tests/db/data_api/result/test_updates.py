from __future__ import annotations

import uuid
from http import HTTPStatus

from bi_constants.enums import FieldType

from bi_api_client.dsmaker.primitives import Dataset
from bi_api_client.dsmaker.shortcuts.result_data import get_data_rows


def test_result_with_updates(api_v1, data_api_v1, static_dataset_id):
    ds = api_v1.load_dataset(dataset=Dataset(id=static_dataset_id)).dataset
    id_1, id_2, id_3 = (str(uuid.uuid4()) for _ in range(3))
    result_resp = data_api_v1.get_result(
        dataset=ds,
        updates=[
            ds.field(id=id_1, title='First', formula='SUM([Sales]) / 100', type=FieldType.MEASURE).add(),
            # use an invalid field type for the second one to make sure it fixes itself
            ds.field(id=id_2, title='Second', formula='COUNTD([Category])', type=FieldType.DIMENSION).add(),
            ds.field(id=id_3, title='Third', formula='[First] / [Second]', type=FieldType.MEASURE).add(),
        ],
        fields=[
            ds.find_field(title='Order Date'),
            ds.field(id=id_1),
            ds.field(id=id_2),
            ds.field(id=id_3),
        ]
    )
    assert result_resp.status_code == HTTPStatus.OK, result_resp.response_errors
    result_data = result_resp.data
    titles = [col_schema[0] for col_schema in result_data['Type'][1][1]]
    assert titles == [
        'Order Date',
        'First', 'Second', 'Third',
    ]


def test_get_result_add_update_field_without_avatar(api_v1, data_api_all_v, static_dataset_id):
    data_api = data_api_all_v
    ds = api_v1.load_dataset(dataset=Dataset(id=static_dataset_id)).dataset

    ds.result_schema['Some Field'] = ds.field(formula='NOW()')

    result_resp = data_api.get_result(
        dataset=ds,
        updates=[
            ds.result_schema['Some Field'].update(calc_mode='direct'),
        ],
        fields=[
            ds.find_field(title='Order Date'),
        ]
    )
    assert result_resp.status_code == HTTPStatus.OK, result_resp.response_errors
    data_rows = get_data_rows(result_resp)
    assert data_rows
