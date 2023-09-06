from __future__ import annotations

import uuid
from http import HTTPStatus

from bi_api_client.dsmaker.primitives import Dataset

from bi_core_testing.database import make_table, C as TestColumn

from bi_legacy_test_bundle_tests.api_lib.utils import data_source_settings_from_table


def _make_name() -> str:
    return str(uuid.uuid4())


def test_sources_with_different_columns(clickhouse_db, connection_id, api_v1):
    db = clickhouse_db
    old_table = make_table(db=db, name=_make_name(), columns=[
        TestColumn.int_value('int_1'),
        TestColumn.int_value('int_2')
    ])
    new_table = make_table(db=db, name=_make_name(), columns=[
        TestColumn.int_value('int_1'),
        TestColumn.int_value('int_3')
    ])

    ds = Dataset()
    ds.sources['old_source'] = ds.source(connection_id=connection_id, **data_source_settings_from_table(old_table))
    ds.sources['new_source'] = ds.source(connection_id=connection_id, **data_source_settings_from_table(new_table))
    ds.source_avatars['avatar'] = ds.sources['old_source'].avatar()
    ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.OK, ds_resp.response_errors
    ds = ds_resp.dataset
    assert len(ds.result_schema) == 2  # 2 columns => 2 fields: int_1, int_2

    ds.result_schema['formula_field_1'] = ds.field(formula='[int_2] + 1')
    ds.result_schema['formula_field_2'] = ds.field(formula='[formula_field_1] + [int_1] + 1')
    ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.OK, ds_resp.response_errors
    ds = ds_resp.dataset
    assert len(ds.result_schema) == 4  # added formula_field

    ds_resp = api_v1.apply_updates(dataset=ds, updates=[
        ds.source_avatars['avatar'].update(source_id=ds.sources['new_source'].id)
    ], fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.BAD_REQUEST
    ds = ds_resp.dataset
    assert len(ds.result_schema) == 5  # added int_3
    assert ds.find_field(title='int_1').valid
    assert not ds.find_field(title='int_2').valid  # refers to an invalid column
    assert not ds.find_field(title='formula_field_1').valid  # refers to a field that refers to an invalid column
    assert not ds.find_field(title='formula_field_2').valid  # ... and so on
    assert ds.find_field(title='int_3').valid

    ds_resp = api_v1.apply_updates(dataset=ds, updates=[
        ds.find_field(title='int_2').delete(),
        ds.find_field(title='formula_field_1').update(formula='[int_3] + 1'),
    ], fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.OK, ds_resp.response_errors


def test_single_src_with_tables_having_different_columns(clickhouse_db, connection_id, api_v1):
    db = clickhouse_db
    old_table = make_table(db=db, name=_make_name(), columns=[
        TestColumn.int_value('int_1'),
        TestColumn.int_value('int_2')
    ])
    new_table = make_table(db=db, name=_make_name(), columns=[
        TestColumn.int_value('int_1'),
        TestColumn.int_value('int_3')
    ])

    ds = Dataset()
    ds.sources['source'] = ds.source(connection_id=connection_id, **data_source_settings_from_table(old_table))
    ds.source_avatars['avatar'] = ds.sources['source'].avatar()
    ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.OK, ds_resp.response_errors
    ds = ds_resp.dataset
    assert len(ds.result_schema) == 2  # 2 columns => 2 fields: int_1, int_2

    ds.result_schema['formula_field_1'] = ds.field(formula='[int_1] + 1')
    ds.result_schema['formula_field_2'] = ds.field(formula='[int_2] + 1')
    ds.result_schema['formula_field_x'] = ds.field(formula='[formula_field_1] + [formula_field_2] + 1')
    ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.OK, ds_resp.response_errors
    ds = ds_resp.dataset
    assert len(ds.result_schema) == 5  # added formula_fields

    # Replace table in source
    ds_resp = api_v1.apply_updates(dataset=ds, updates=[
        ds.sources['source'].update(
            parameters=dict(ds.sources['source'].parameters, table_name=new_table.name)
        ),
        ds.source_avatars['avatar'].update(
            title=new_table.name,
            source_id=ds.source_avatars['avatar'].source_id,  # to make it look like we're updating more than just the title
        ),
    ], fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.BAD_REQUEST
    ds = ds_resp.dataset
    assert len(ds.result_schema) == 6  # added int_3
    assert ds.find_field(title='int_1').valid
    assert ds.find_field(title='formula_field_1').valid
    assert not ds.find_field(title='int_2').valid  # refers to an invalid column
    assert not ds.find_field(title='formula_field_2').valid  # refers to a field that refers to an invalid column
    assert not ds.find_field(title='formula_field_x').valid  # ... and so on
    assert ds.find_field(title='int_3').valid

    ds_resp = api_v1.apply_updates(dataset=ds, updates=[
        ds.find_field(title='int_2').delete(),
        ds.find_field(title='formula_field_2').update(formula='[int_3] + 1'),
    ], fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.OK, ds_resp.response_errors
