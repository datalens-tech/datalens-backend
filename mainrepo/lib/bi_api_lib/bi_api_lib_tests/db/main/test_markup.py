from __future__ import annotations

import uuid

from bi_api_lib_tests.utils import get_result_schema


def mk_id():
    return str(uuid.uuid4())


def test_markup(client, static_dataset_id, data_api_v1):
    ds_id = static_dataset_id
    result_schema = get_result_schema(client, ds_id)
    title_to_id = {field['title']: field['guid'] for field in result_schema}

    field_a = mk_id()
    formula_a = '''
        markup(
            italic(url(
                "http://example.com/?city=" + [City] + "&_=1",
                [City])),
            " (", bold(str([Postal Code])), ")")
    '''
    field_b = mk_id()
    formula_b = '''
        url("https://yandex.ru/search/?text=" + str([Profit]) + " usd в рублях", str([Profit]))
    '''
    field_nulled = mk_id()
    formula_nulled = '''
        url(if([Profit] > 0, NULL, "neg"), str([Profit]))
    '''
    batch = [
        {'action': 'add', 'field': {
            'guid': field_a, 'title': 'Field A',
            'calc_mode': 'formula', 'formula': formula_a,
        }},
        {'action': 'add', 'field': {
            'guid': field_b, 'title': 'Field B',
            'calc_mode': 'formula', 'formula': formula_b,
        }},
        {'action': 'add', 'field': {
            'guid': field_nulled, 'title': 'Field Nulled',
            'calc_mode': 'formula', 'formula': formula_nulled,
        }},
    ]
    cols = [title_to_id['Category'], field_a, field_b, field_nulled]
    req_data = {
        'updates': batch,
        'columns': cols,
        'group_by': cols,
    }
    resp = data_api_v1.get_response_for_dataset_result(
        dataset_id=ds_id, raw_body=req_data)
    assert resp.status_code == 200, resp.json
    rd = resp.json['result']
    assert rd

    some_row = rd['data']['Data'][0]
    assert len(some_row) == len(cols)
    res_cat, res_a, res_b, res_nulled = some_row
    assert isinstance(res_cat, str)
    assert isinstance(res_a, dict)
    assert isinstance(res_b, dict)
    assert res_nulled is None
    b_val = res_b['content']['content']
    assert isinstance(b_val, str)
    expected_b = {
        'type': 'url',
        'url': f'https://yandex.ru/search/?text={b_val} usd в рублях',
        'content': {'type': 'text', 'content': b_val}}
    assert res_b == expected_b
