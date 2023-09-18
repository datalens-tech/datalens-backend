from __future__ import annotations

import uuid
from http import HTTPStatus

import pytest

import dl_query_processing.exc
from dl_api_lib import exc
from dl_constants.enums import TopLevelComponentId

from dl_api_client.dsmaker.primitives import Dataset
from dl_api_client.dsmaker.api.dataset_api import SyncHttpDatasetApiV1

from dl_core.constants import DatasetConstraints
from dl_core_testing.database import make_table, C

from bi_legacy_test_bundle_tests.api_lib.utils import data_source_settings_from_table


TEST_FIELD_LIMIT_SOFT = 10
TEST_FIELD_LIMIT_HARD = 15


@pytest.fixture(scope='function')
def patched_limits(monkeypatch) -> None:
    monkeypatch.setattr(DatasetConstraints, 'FIELD_COUNT_LIMIT_SOFT', TEST_FIELD_LIMIT_SOFT)
    monkeypatch.setattr(DatasetConstraints, 'FIELD_COUNT_LIMIT_HARD', TEST_FIELD_LIMIT_HARD)


def test_dataset_field_limit_add_formula(
        patched_limits, api_v1: SyncHttpDatasetApiV1,
        clickhouse_db, static_connection_id,
):
    limit_soft = DatasetConstraints.FIELD_COUNT_LIMIT_SOFT
    limit_hard = DatasetConstraints.FIELD_COUNT_LIMIT_HARD

    table = make_table(db=clickhouse_db, name=str(uuid.uuid4()), columns=[C.int_value(name='int_value')])
    ds = Dataset()
    ds.sources['source_1'] = ds.source(connection_id=static_connection_id, **data_source_settings_from_table(table))
    ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()

    # add OK number of fields
    for i in range(limit_soft - 1):
        ds.result_schema[f'formula_{i}'] = ds.field(formula=f'[int_value] + {i}')
    ds_resp = api_v1.apply_updates(dataset=ds)
    assert ds_resp.status_code == HTTPStatus.OK
    ds = ds_resp.dataset
    assert len(ds.result_schema) == limit_soft
    for field in ds.result_schema:
        assert field.valid

    # add one more field so result schema length exceeds the soft limit
    ds.result_schema['formula_soft'] = ds.field(formula=f'[int_value] + {limit_soft - 1}')
    ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True)
    ds = ds_resp.dataset
    assert ds_resp.status_code == HTTPStatus.BAD_REQUEST
    result_schema_errors = ds.component_errors.get_pack(id=TopLevelComponentId.result_schema.value).errors
    assert len(result_schema_errors) == 1
    assert result_schema_errors[0].code == 'ERR.DS_API.' + '.'.join(exc.TooManyFieldsError.err_code)

    # try saving and confirm failure
    ds_resp = api_v1.save_dataset(dataset=ds, fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.BAD_REQUEST
    assert ds_resp.bi_status_code == 'ERR.DS_API.' + '.'.join(
        dl_query_processing.exc.DatasetTooManyFieldsFatal.err_code)

    # remove one field and confirm that result schema is ok
    ds_resp = api_v1.apply_updates(dataset=ds, updates=[ds.find_field(title='formula_0').delete()])
    assert ds_resp.status_code == HTTPStatus.OK
    ds = ds_resp.dataset
    assert ds.component_errors.get_pack(id=TopLevelComponentId.result_schema.value) is None

    # add some more fields to test there is only one error
    more_field_cnt = 3
    # just a precaution for fine-tuning the test correctly:
    assert len(ds.result_schema) + more_field_cnt < TEST_FIELD_LIMIT_HARD
    for i in range(more_field_cnt):
        ds.result_schema[f'formula_stage_1_soft_{i}'] = ds.field(formula=f'[int_value] + {i}')
    ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.BAD_REQUEST
    ds = ds_resp.dataset
    assert len(ds.component_errors.items) == 1

    # add more fields to exceed the hard limit
    for i in range(limit_hard - limit_soft):
        ds.result_schema[f'formula_stage_2_soft_{i}'] = ds.field(formula=f'[int_value] + {i}')
    ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True)
    assert ds_resp.bi_status_code == 'ERR.DS_API.' + '.'.join(
        dl_query_processing.exc.DatasetTooManyFieldsFatal.err_code)


def test_dataset_field_limit_add_source(
        patched_limits, api_v1: SyncHttpDatasetApiV1,
        clickhouse_db, static_connection_id,
):
    limit_soft = DatasetConstraints.FIELD_COUNT_LIMIT_SOFT
    limit_hard = DatasetConstraints.FIELD_COUNT_LIMIT_HARD

    table = make_table(db=clickhouse_db, name=str(uuid.uuid4()), columns=[C.int_value(name='single_column')])
    ds = Dataset()
    ds.sources['source_1'] = ds.source(connection_id=static_connection_id, **data_source_settings_from_table(table))

    def add_avatar_and_relation(suffix: str) -> None:
        ds.source_avatars[f'avatar{suffix}'] = ds.sources['source_1'].avatar()
        ds.avatar_relations[f'relation{suffix}'] = ds.source_avatars['avatar_ok_0'].join(
            ds.source_avatars[f'avatar{suffix}']
        ).on(
            ds.col('single_column') == ds.col('single_column')
        )

    # add avatars that produce OK length result schema
    for i in range(limit_soft):
        add_avatar_and_relation(suffix=f'_ok_{i}')
    ds_resp = api_v1.apply_updates(dataset=ds)
    assert ds_resp.status_code == HTTPStatus.OK
    ds = ds_resp.dataset
    ds = api_v1.save_dataset(dataset=ds).dataset

    # add another avatar that makes the result schema length exceed the soft limit resulting in a non-fatal error
    add_avatar_and_relation(suffix='_soft')
    ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.BAD_REQUEST
    ds = ds_resp.dataset
    result_schema_errors = ds.component_errors.get_pack(TopLevelComponentId.result_schema.value).errors
    assert len(result_schema_errors) == 1
    assert result_schema_errors[0].code == 'ERR.DS_API.' + '.'.join(exc.TooManyFieldsError.err_code)

    # try saving and confirm failure
    ds_resp = api_v1.save_dataset(dataset=ds, fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.BAD_REQUEST
    assert ds_resp.bi_status_code == 'ERR.DS_API.' + '.'.join(
        dl_query_processing.exc.DatasetTooManyFieldsFatal.err_code)

    # remove one avatar and confirm that the result schema is ok
    ds_resp = api_v1.apply_updates(dataset=ds, updates=[
        ds.avatar_relations['relation_ok_1'].delete(),
        ds.source_avatars['avatar_ok_1'].delete(),
    ])
    assert ds_resp.status_code == HTTPStatus.OK
    ds = ds_resp.dataset

    # add more avatar which makes the result schema length exceed the hard limit resulting in a fatal error
    for i in range(limit_hard - limit_soft):
        add_avatar_and_relation(suffix=f'_soft_{i}')
    add_avatar_and_relation(suffix='_fatal')
    ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.BAD_REQUEST
    assert ds_resp.bi_status_code == 'ERR.DS_API.' + '.'.join(
        dl_query_processing.exc.DatasetTooManyFieldsFatal.err_code)


def test_dataset_field_limit_refresh_source(
        patched_limits, api_v1: SyncHttpDatasetApiV1,
        clickhouse_db, static_connection_id,
):
    limit_soft = DatasetConstraints.FIELD_COUNT_LIMIT_SOFT
    limit_hard = DatasetConstraints.FIELD_COUNT_LIMIT_HARD

    table_name = str(uuid.uuid4())
    table = make_table(
        db=clickhouse_db,
        name=table_name,
        columns=[C.int_value(name=f'col_{i}') for i in range(limit_soft)]
    )
    ds = Dataset()
    ds.sources['source_1'] = ds.source(connection_id=static_connection_id, **data_source_settings_from_table(table))

    ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()

    # add OK number of fields from the source table columns
    ds_resp = api_v1.apply_updates(dataset=ds)
    assert ds_resp.status_code == HTTPStatus.OK
    ds = ds_resp.dataset
    assert len(ds.result_schema) == limit_soft
    ds = api_v1.save_dataset(dataset=ds).dataset

    # add a column to the source table and refresh data source so result schema exceeds the soft limit
    make_table(
        db=clickhouse_db,
        name=table_name,
        columns=[C.int_value(name=f'col_{i}') for i in range(limit_soft + 1)]
    )
    ds_resp = api_v1.apply_updates(dataset=ds, updates=[ds.sources['source_1'].refresh()], fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.BAD_REQUEST
    ds = ds_resp.dataset
    result_schema_errors = ds.component_errors.get_pack(TopLevelComponentId.result_schema.value).errors
    assert len(result_schema_errors) == 1
    assert result_schema_errors[0].code == 'ERR.DS_API.' + '.'.join(exc.TooManyFieldsError.err_code)

    # try saving and confirm failure
    ds_resp = api_v1.save_dataset(dataset=ds, fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.BAD_REQUEST
    assert ds_resp.bi_status_code == 'ERR.DS_API.' + '.'.join(
        dl_query_processing.exc.DatasetTooManyFieldsFatal.err_code)

    # remove last column from the table, refresh source and confirm that result schema is ok
    make_table(
        db=clickhouse_db,
        name=table_name,
        columns=[C.int_value(name=f'col_{i}') for i in range(limit_soft)]
    )
    ds_resp = api_v1.apply_updates(
        dataset=ds,
        updates=[
            ds.sources['source_1'].refresh(),
            ds.find_field(title=f'col_{limit_soft}').delete()
        ]
    )
    assert ds_resp.status_code == HTTPStatus.OK
    ds = ds_resp.dataset
    ds_resp = api_v1.save_dataset(dataset=ds, fail_ok=True)
    ds = ds_resp.dataset
    assert ds_resp.status_code == HTTPStatus.OK

    # add more columns to the source table so that refreshing the data source results in a fatal error
    make_table(
        db=clickhouse_db,
        name=table_name,
        columns=[C.int_value(name=f'col_{i}') for i in range(limit_hard + 1)]
    )
    ds_resp = api_v1.apply_updates(dataset=ds, updates=[ds.sources['source_1'].refresh()], fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.BAD_REQUEST
    assert ds_resp.bi_status_code == 'ERR.DS_API.' + '.'.join(
        dl_query_processing.exc.DatasetTooManyFieldsFatal.err_code)
