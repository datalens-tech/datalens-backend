from __future__ import annotations

import datetime
from collections import defaultdict
from http import HTTPStatus
from statistics import mean
from typing import Dict, List, NamedTuple, Union

import pytest

from bi_constants.enums import WhereClauseOperation, FieldRole

from bi_api_client.dsmaker.primitives import Dataset
from bi_api_client.dsmaker.api.data_api import SyncHttpDataApiV1, SyncHttpDataApiV1_5
from bi_api_client.dsmaker.shortcuts.dataset import add_formulas_to_dataset
from bi_api_client.dsmaker.shortcuts.result_data import get_data_rows


class TotalsTestEnv(NamedTuple):
    dataset: Dataset
    data_api: Union[SyncHttpDataApiV1, SyncHttpDataApiV1_5]
    dimensions: List[str]
    measures: List[str]
    batch: List[dict]


@pytest.fixture
def totals_test(api_v1, data_api_legacy, client, static_dataset_id) -> TotalsTestEnv:
    data_api = data_api_legacy
    dataset_id = static_dataset_id
    ds = api_v1.load_dataset(dataset=Dataset(id=dataset_id)).dataset

    dimensions = ['Category', 'City']
    measures = ['Discount', 'Postal Code', 'Profit', 'Quantity', 'Row ID', 'Sales', 'Ship Date']

    batch = [
        field.update(aggregation='avg')
        for field in ds.result_schema
        if field.title in measures
    ]
    ds.result_schema['count'] = ds.field(formula='count()')
    measures += ['count']

    return TotalsTestEnv(
        data_api=data_api,
        dataset=ds,
        dimensions=dimensions,
        measures=measures,
        batch=batch,
    )


def test_get_dataset_version_result_with_totals(totals_test: TotalsTestEnv):
    # Reference (maybe should be in the test):
    #
    #     import pandas as pd
    #     df = pd.read_csv(
    #         './docker-compose/db-common/data/sample.csv',
    #         header=None,
    #         names=[
    #             "Category", "City", "Country", "Customer ID", "Customer Name",
    #             "Discount", "Order Date", "Order ID", "Postal Code", "Product ID",
    #             "Product Name", "Profit", "Quantity", "Region", "Row ID", "Sales",
    #             "Segment", "Ship Date", "Ship Mode", "State", "Sub-Category"])
    #     df = df[df['City'].str.startswith('Wa')]
    #     df.groupby(['Category', 'City']).sum(), \
    #     df[['Discount', 'Postal Code', 'Profit', 'Quantity', 'Row ID', 'Sales']].mean()

    tt = totals_test
    ds = tt.dataset
    result_resp = tt.data_api.get_result(
        dataset=ds,
        fields=[ds.find_field(title=title) for title in tt.dimensions + tt.measures],
        order_by=[ds.find_field(title=title).desc for title in tt.dimensions[:2]],
        filters=[ds.find_field(title='City').filter(WhereClauseOperation.STARTSWITH, values=['Wa'])],
        updates=tt.batch,
        with_totals=True,
    )
    assert result_resp.status_code == HTTPStatus.OK
    rd = result_resp.json['result']
    assert rd['totals']

    count_idx = next(
        idx
        for idx, col in enumerate(rd['data']['Type'][-1][-1])
        if col[0] == 'count')
    for idx, totals_value in enumerate(rd['totals']):
        if totals_value is None:
            continue
        type_name = rd['data']['Type'][-1][-1][idx][-1][-1][-1]
        if type_name == 'Date':
            totals_value = datetime.date.fromisoformat(totals_value)
            expected_value = datetime.date.fromordinal(int(mean(
                datetime.date.fromisoformat(row[idx]).toordinal()
                for row in rd['data']['Data']
                # assuming there isn't a huge amount of source rows
                for _ in range(int(row[count_idx]))
            )))
            assert expected_value == totals_value, f'totals mismatch at date column {idx}'
        else:
            totals_value = float(totals_value)
            expected_value = mean(
                float(row[idx])
                for row in rd['data']['Data']
                # assuming there isn't a huge amount of source rows
                for _ in range(int(row[count_idx]))
            )
            assert (expected_value - totals_value) < 0.01, f'totals mismatch at column {idx}'


def test_get_dataset_version_result_with_totals_no_dims(totals_test):
    tt = totals_test
    ds = tt.dataset
    result_resp = tt.data_api.get_result(
        dataset=ds,
        fields=[ds.find_field(title=title) for title in tt.measures],
        updates=tt.batch,
        with_totals=True,
    )
    assert result_resp.status_code == HTTPStatus.OK
    rd = result_resp.json['result']
    assert rd['totals']


def test_get_dataset_version_result_with_totals_no_meas(totals_test):
    tt = totals_test
    ds = tt.dataset
    result_resp = tt.data_api.get_result(
        dataset=ds,
        fields=[ds.find_field(title=title) for title in tt.dimensions],
        order_by=[ds.find_field(title=title).desc for title in tt.dimensions[:2]],
        filters=[ds.find_field(title='City').filter(WhereClauseOperation.STARTSWITH, values=['Wa'])],
        updates=tt.batch,
        with_totals=True,
    )
    assert result_resp.status_code == HTTPStatus.OK
    rd = result_resp.json['result']
    assert rd['totals']


def test_get_dataset_version_result_with_totals_no_data(totals_test):
    tt = totals_test
    ds = tt.dataset
    result_resp = tt.data_api.get_result(
        dataset=ds,
        fields=[ds.find_field(title=title) for title in tt.dimensions + tt.measures],
        order_by=[ds.find_field(title=title).desc for title in tt.dimensions[:2]],
        filters=[ds.find_field(title='City').filter(WhereClauseOperation.IN, values=['NonexistentCity'])],
        updates=tt.batch,
        with_totals=True,
    )
    assert result_resp.status_code == HTTPStatus.OK
    rd = result_resp.json['result']
    assert rd['data']['Data'] == []
    assert rd['totals'] is None or len(rd['totals']) == len(tt.dimensions + tt.measures)


def test_v2_totals(api_v1, data_api_v2, dataset_id):
    data_api = data_api_v2
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Sales Sum': 'SUM([Sales])',
    })

    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            # Main
            ds.find_field(title='Category').as_req_legend_item(block_id=0, legend_item_id=0),
            ds.find_field(title='Sales Sum').as_req_legend_item(block_id=0, legend_item_id=1),
            # Total
            ds.placeholder_as_req_legend_item(
                role=FieldRole.template, template='Total', block_id=1, legend_item_id=2),
            ds.find_field(title='Sales Sum').as_req_legend_item(
                role=FieldRole.total, block_id=1, legend_item_id=3),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK, result_resp.json

    raw_rows = result_resp.json['result_data'][0]['rows']
    main_data = raw_rows[:-1]
    assert main_data
    for row in main_data:
        assert row['legend'][0] == 0
        assert row['legend'][1] == 1

    total_row = raw_rows[-1]
    assert total_row['legend'][0] == 2
    assert total_row['legend'][1] == 3
    assert total_row['data'][0] == 'Total'
    assert float(total_row['data'][1]) == sum([float(row['data'][1]) for row in main_data])


def test_v2_totals_no_measures(api_v1, data_api_v2, dataset_id):
    data_api = data_api_v2
    ds = api_v1.load_dataset(dataset=Dataset(id=dataset_id)).dataset

    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            # Main
            ds.find_field(title='Category').as_req_legend_item(block_id=0, legend_item_id=0),
            ds.find_field(title='City').as_req_legend_item(block_id=0, legend_item_id=1),
            # Total
            ds.placeholder_as_req_legend_item(role=FieldRole.template, template='-', block_id=1, legend_item_id=2),
            ds.placeholder_as_req_legend_item(role=FieldRole.template, template='-', block_id=1, legend_item_id=3),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK, result_resp.json

    raw_rows = result_resp.json['result_data'][0]['rows']

    total_row = raw_rows[-1]
    assert total_row['legend'][0] == 2
    assert total_row['legend'][1] == 3
    assert total_row['data'][0] == '-'
    assert total_row['data'][1] == '-'


def test_v2_subtotals_with_placeholders(api_v1, data_api_v2, dataset_id):
    data_api = data_api_v2
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Sales Sum': 'SUM([Sales])',
    })
    cat_field_id = ds.find_field(title='Category').id

    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            # Main
            ds.find_field(title='City').as_req_legend_item(block_id=0, legend_item_id=0),
            ds.find_field(title='Category').as_req_legend_item(block_id=0, legend_item_id=1),
            ds.find_field(title='Sales Sum').as_req_legend_item(block_id=0, legend_item_id=2),
            # Subtotals
            ds.placeholder_as_req_legend_item(
                role=FieldRole.template, template='{{ '+cat_field_id+' }} Subtotal',
                block_id=1, legend_item_id=3),
            ds.find_field(title='Category').as_req_legend_item(
                block_id=1, legend_item_id=4),
            ds.find_field(title='Sales Sum').as_req_legend_item(
                role=FieldRole.total, block_id=1, legend_item_id=5),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK, result_resp.json

    raw_rows = result_resp.json['result_data'][0]['rows']

    main_rows = [row for row in raw_rows if row['legend'] == [0, 1, 2]]
    subtotal_rows = [row for row in raw_rows if row['legend'] == [3, 4, 5]]

    assert len(main_rows) > 3
    values_by_cat: Dict[str, List[float]] = defaultdict(list)
    for row in main_rows:
        values_by_cat[row['data'][1]].append(float(row['data'][2]))

    assert len(subtotal_rows) == 3
    for row in subtotal_rows:
        placeholder = row['data'][0]
        cat = row['data'][1]
        subtotal_sum = float(row['data'][2])
        assert subtotal_sum == pytest.approx(sum(values_by_cat[cat]))
        assert placeholder == f'{cat} Subtotal'


def test_totals_with_measure_filters(api_v1, data_api_v2, dataset_id):
    """
    Check that totals are ignored if measure filters are present

    https://st.yandex-team.ru/BI-2585
    """

    data_api = data_api_v2
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Sales Sum': 'SUM([Sales])',
    })
    cat_field_id = ds.find_field(title='Category').id

    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            # Main
            ds.find_field(title='City').as_req_legend_item(block_id=0, legend_item_id=0),
            ds.find_field(title='Category').as_req_legend_item(block_id=0, legend_item_id=1),
            ds.find_field(title='Sales Sum').as_req_legend_item(block_id=0, legend_item_id=2),
            # Subtotals
            ds.placeholder_as_req_legend_item(
                role=FieldRole.template, template='{{ '+cat_field_id+' }} Subtotal', block_id=1),
            ds.find_field(title='Category').as_req_legend_item(block_id=1),
            ds.find_field(title='Sales Sum').as_req_legend_item(role=FieldRole.total, block_id=1),
        ],
        filters=[
            ds.find_field(title='Sales Sum').filter(WhereClauseOperation.GT, [0.1]),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK, result_resp.json

    all_rows = result_resp.json['result_data'][0]['rows']

    main_rows = [row for row in all_rows if row['legend'] == [0, 1, 2]]

    assert main_rows == all_rows

    assert len(result_resp.json['notifications']) == 1
    assert result_resp.json['notifications'][0]['locator'] == 'totals_removed_due_to_measure_filter'


def test_v2_subtotals(api_v1, data_api_v2, dataset_id):
    data_api = data_api_v2
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Sales Sum': 'SUM([Sales])',
        'Count': 'COUNT([Sales])',
    })
    reg_field_id = ds.find_field(title='Region').id
    cat_field_id = ds.find_field(title='Category').id

    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            # Main
            ds.find_field(title='Region').as_req_legend_item(block_id=0, legend_item_id=0),
            ds.find_field(title='Category').as_req_legend_item(block_id=0, legend_item_id=1),
            ds.find_field(title='City').as_req_legend_item(block_id=0, legend_item_id=2),
            ds.find_field(title='Sales Sum').as_req_legend_item(block_id=0, legend_item_id=3),
            ds.find_field(title='Count').as_req_legend_item(block_id=0, legend_item_id=4),
            # City sub-totals
            ds.find_field(title='Region').as_req_legend_item(block_id=1, legend_item_id=10),
            ds.find_field(title='Category').as_req_legend_item(block_id=1, legend_item_id=11),
            ds.placeholder_as_req_legend_item(
                role=FieldRole.template, template='{{ '+cat_field_id+' }} Subtotal', block_id=1, legend_item_id=12),
            ds.find_field(title='Sales Sum').as_req_legend_item(role=FieldRole.total, block_id=1, legend_item_id=13),
            ds.find_field(title='Count').as_req_legend_item(block_id=1, legend_item_id=14),
            # Region sub-totals
            ds.find_field(title='Region').as_req_legend_item(block_id=2, legend_item_id=20),
            ds.placeholder_as_req_legend_item(
                role=FieldRole.template, template='{{ ' + reg_field_id + ' }} Subtotal', block_id=2, legend_item_id=21),
            ds.placeholder_as_req_legend_item(
                role=FieldRole.template, template='-', block_id=2, legend_item_id=22),
            ds.find_field(title='Sales Sum').as_req_legend_item(role=FieldRole.total, block_id=2, legend_item_id=23),
            ds.find_field(title='Count').as_req_legend_item(block_id=2, legend_item_id=24),
            # Grand Total
            ds.placeholder_as_req_legend_item(
                role=FieldRole.template, template='Total', block_id=3, legend_item_id=30),
            ds.placeholder_as_req_legend_item(
                role=FieldRole.template, template='-', block_id=3, legend_item_id=31),
            ds.placeholder_as_req_legend_item(
                role=FieldRole.template, template='-', block_id=3, legend_item_id=32),
            ds.find_field(title='Sales Sum').as_req_legend_item(role=FieldRole.total, block_id=3, legend_item_id=33),
            ds.find_field(title='Count').as_req_legend_item(block_id=3, legend_item_id=34),
        ],
        order_by=[
            # Only main block is ordered
            ds.find_field(title='Region').asc.for_block(block_id=0),
            ds.find_field(title='Category').asc.for_block(block_id=0),
            ds.find_field(title='City').asc.for_block(block_id=0),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK, result_resp.json

    raw_rows = result_resp.json['result_data'][0]['rows']
    cat_sub_sales_total = 0.0
    cat_sub_cnt_total = 0
    reg_sub_sales_total = 0.0
    reg_sub_cnt_total = 0
    grand_sales_total = 0.0
    grand_cnt_total = 0
    for row in raw_rows:
        sales = float(row['data'][3])
        cnt = int(row['data'][4])
        if row['legend'] == [0, 1, 2, 3, 4]:
            cat_sub_sales_total += sales
            cat_sub_cnt_total += cnt
            reg_sub_sales_total += sales
            reg_sub_cnt_total += cnt
            grand_sales_total += sales
            grand_cnt_total += cnt
        elif row['legend'] == [10, 11, 12, 13, 14]:
            # Category sub-total row
            assert sales == cat_sub_sales_total
            assert cnt == cat_sub_cnt_total
            cat_sub_sales_total = 0
            cat_sub_cnt_total = 0
        elif row['legend'] == [20, 21, 22, 23, 24]:
            # Region sub-total row
            assert sales == reg_sub_sales_total
            assert cnt == reg_sub_cnt_total
            reg_sub_sales_total = 0
            reg_sub_cnt_total = 0
        elif row['legend'] == [30, 31, 32, 33, 34]:
            # Grand total row
            assert sales == grand_sales_total
            assert cnt == grand_cnt_total

        assert grand_sales_total > 0
        assert grand_cnt_total > 0


def test_nullified_totals_for_complex_query(api_v1, data_api_v2, dataset_id):
    """
    Totals should be disabled for:
    - inconsistent aggregations
    - lookups
    - ext. aggs
    - window functions
    """

    data_api = data_api_v2
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Extended Agg': 'AVG(SUM([Sales] INCLUDE [City]))',
        'Lookup Agg': 'AGO(SUM([Sales]), [Order Date])',
        'Inconsistent Agg': 'SUM([Sales]) * INT([Order Date])',
        'Window Agg': 'LAG(SUM([Sales]))',
    })

    def test_for_measure(measure_name: str) -> None:
        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                # Main
                ds.find_field(title='Order Date').as_req_legend_item(block_id=0),
                ds.find_field(title=measure_name).as_req_legend_item(block_id=0),
                # Subtotals
                ds.placeholder_as_req_legend_item(role=FieldRole.template, template='-', block_id=1),
                ds.find_field(title=measure_name).as_req_legend_item(role=FieldRole.total, block_id=1),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json

        all_rows = get_data_rows(result_resp)
        total_row = all_rows[-1]

        assert total_row[0] == '-'
        assert total_row[1] is None

    test_for_measure('Extended Agg')
    test_for_measure('Inconsistent Agg')
    test_for_measure('Lookup Agg')
    test_for_measure('Window Agg')
