from __future__ import annotations

from http import HTTPStatus

from bi_constants.enums import AggregationFunction, BIType, ConnectionType, CreateDSFrom

from bi_api_client.dsmaker.primitives import Dataset

from bi_api_lib.enums import WhereClauseOperation, BI_TYPE_AGGREGATIONS

from bi_connector_postgresql.core.postgresql.constants import CONNECTION_TYPE_POSTGRES
from bi_connector_mssql.core.constants import CONNECTION_TYPE_MSSQL

from bi_legacy_test_bundle_tests.api_lib.utils import data_source_settings_from_table


def test_basic(api_v1, ch_data_source_settings, ch_other_data_source_settings):
    ds = Dataset()
    ds.sources['source_1'] = ds.source(**ch_data_source_settings)
    ds.sources['source_2'] = ds.source(**ch_other_data_source_settings)
    ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()
    ds.source_avatars['avatar_2'] = ds.sources['source_2'].avatar()
    ds.avatar_relations['relation_1'] = ds.source_avatars['avatar_1'].join(
        ds.source_avatars['avatar_2']
    ).on(
        ds.col('Order ID') == ds.col('Order ID')
    )
    ds_resp = api_v1.apply_updates(dataset=ds)
    assert ds_resp.status_code == HTTPStatus.OK
    ds = ds_resp.dataset

    options = ds_resp.json['options']
    assert options['schema_update_enabled'] is True
    assert options['preview']['enabled'] is True
    assert len(options['join']['types']) == 4
    assert options['join']['operators'] == ['eq']
    assert (
        {'source_type': CreateDSFrom.CH_TABLE.name}
        in options['sources']['compatible_types']
    )
    assert len(options['sources']['items']) == 2
    assert options['sources']['items'][0]['schema_update_enabled'] is True
    compat_conn_types = {
        item['conn_type'] for item in options['connections']['compatible_types']
    }
    assert set(compat_conn_types) == set()  # Cannot combine multiple connections
    assert options['connections']['max'] == 1
    assert options['source_avatars']['max'] > 1
    assert len(options['source_avatars']['items']) == 2
    assert options['source_avatars']['items'][0]['schema_update_enabled'] is True
    assert WhereClauseOperation.EQ.name in options['data_types']['items'][0]['filter_operations']
    for item in options['data_types']['items']:
        assert set(item['aggregations']) \
               == set(x.name for x in BI_TYPE_AGGREGATIONS[BIType[item['type']]] if x != AggregationFunction.none)

    # Regular functions
    assert {'max', 'concat'}.issubset(set(options['supported_functions']))
    # Window functions
    assert {'mmax', 'rsum'}.issubset(set(options['supported_functions']))

    # Fields
    assert [f['guid'] for f in options['fields']['items']] == [f.id for f in ds.result_schema]


def test_fields(api_v1, ch_data_source_settings, ch_other_data_source_settings):
    ds = Dataset()
    ds.sources['source_1'] = ds.source(**ch_data_source_settings)
    ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()
    ds_resp = api_v1.apply_updates(dataset=ds)
    assert ds_resp.status_code == HTTPStatus.OK
    ds = ds_resp.dataset

    # Add fields for testing
    ds.result_schema['formula_agg'] = ds.field(formula=f'MAX([{ds.result_schema[0].title}])')
    ds.result_schema['bool'] = ds.field(formula='TRUE')
    ds.result_schema['markup'] = ds.field(formula='BOLD("qwerty")')
    ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.OK
    ds = ds_resp.dataset

    options = ds_resp.json['options']

    def _f_opt(title: str) -> dict:
        ds_field = ds.find_field(title=title)
        return [f for f in options['fields']['items'] if f['guid'] == ds_field.id][0]

    # AggregationFunction
    assert set(_f_opt('formula_agg')['aggregations']) == {
        AggregationFunction.none.name,
    }
    assert set(_f_opt('bool')['aggregations']) == {
        AggregationFunction.none.name,
        AggregationFunction.count.name,
    }

    # Casts
    assert set(_f_opt('bool')['casts']) == {
        BIType.boolean.name,
        BIType.integer.name,
        BIType.float.name,
        BIType.string.name,
    }
    assert set(_f_opt('markup')['casts']) == {
        BIType.markup.name,
    }


def test_direct_connection_replacement_types(
    api_v1, pg_connection_id, postgres_table,
):
    # Initialize DS
    ds = Dataset()
    ds.sources['source_1'] = ds.source(
        **data_source_settings_from_table(postgres_table), connection_id=pg_connection_id)
    ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()
    ds_resp = api_v1.apply_updates(dataset=ds)
    assert ds_resp.status_code == HTTPStatus.OK

    options = ds_resp.json['options']
    conn_items = options['connections']['items']
    assert len(conn_items) == 1
    repl_conn_types = {
        item['conn_type'] for item in conn_items[0]['replacement_types']
    }
    assert {
        CONNECTION_TYPE_POSTGRES.name,
        CONNECTION_TYPE_MSSQL.name,
        ConnectionType.clickhouse.name,
    }.issubset(repl_conn_types)
