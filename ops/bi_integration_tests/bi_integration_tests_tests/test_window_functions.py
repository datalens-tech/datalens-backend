from collections import defaultdict
import logging
from typing import (
    Any,
    Dict,
    List,
)

import pandas as pd
from pandas._testing import (
    assert_frame_equal,
    assert_series_equal,
)
import pytest
from test_data.sales_dataset import (
    INDEX_COLUMN,
    VALUE_COLUMNS,
    read_superstore_csv_as_pandas_df,
)

from bi_integration_tests import datasets
from bi_integration_tests.common import (
    RequestExecutor,
    add_formula_fields_to_dataset,
    setup_db_conn_and_dataset,
)
from bi_integration_tests.request_executors.bi_api_client import BIAPIClient
from bi_integration_tests.sales_table import upload_data_from_df
from bi_testing_ya.api_wrappers import (
    Req,
    Resp,
)
from bi_testing_ya.dlenv import DLEnv
from dl_testing.utils import (
    guids_from_titles,
    skip_outside_devhost,
)


BASE_CLOUD_DIRECTORY = "window_function_tests"

logging.basicConfig()
logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)


async def _create_df_from_datalens_response(
    bi_api_client: BIAPIClient,
    ds_response: Resp,
    field_titles: List[str],
    where_conditions: List[Dict[str, Any]] = None,
) -> pd.DataFrame:
    result_schema = ds_response.json["dataset"]["result_schema"]
    request_data_json = {"columns": guids_from_titles(result_schema, field_titles)}

    if where_conditions:
        request_data_json["where"] = where_conditions

    dataset_id = ds_response.json["id"]
    columns_request = Req(
        "post",
        f"/api/data/v1/datasets/{dataset_id}/versions/draft/result",
        data_json=request_data_json,
    )

    col_values_response = await bi_api_client.execute_request(columns_request)
    field_values = col_values_response.json["result"]["data"]["Data"]
    field_names_to_types = {
        field_name: field_type[1][1]
        for field_name, field_type in col_values_response.json["result"]["data"]["Type"][1][1]
    }

    assert field_values

    datalens_df = pd.DataFrame(field_values, columns=field_names_to_types)
    datalens_to_pandas_types_mapping = {"Double": float, "Int64": int}
    for col_name, col_type in field_names_to_types.items():
        pandas_dtype = datalens_to_pandas_types_mapping.get(col_type, None)
        if pandas_dtype is not None:
            datalens_df[col_name] = datalens_df[col_name].astype(pandas_dtype)

    return datalens_df


@pytest.mark.parametrize(
    "dl_env",
    [
        DLEnv.cloud_preprod,
        DLEnv.cloud_prod,
        DLEnv.internal_preprod,
        DLEnv.internal_prod,
    ],
    indirect=True,
)
@skip_outside_devhost
@pytest.mark.asyncio
async def test_window_functions_db(
    dl_env,
    two_users_configuration,
    integration_tests_reporter,
    integration_tests_folder_id,
    integration_tests_postgres_1,
    ext_sys_requisites,
    workbook_id,
    tenant,
):
    """
    Executes the following steps:
        1) Creates Pandas dataframe from local csv file.
        2) Prepares PG schema and inserts data from Pandas dataframe into integration_tests_sales table
        3) Adds formula fields based on window functions to DataLens dataset.
        4) Downloads the resulting DataLens dataset and creates Pandas dataframe from it.
        5) Compares base columns without window functions to check if data was imported correctly from both sources.
        6) Validates that DataLens windows functions return same values as obtained with Pandas native instruments.
        7) Requests dataset from DataLens with where condition set to test BeforeFilterBy.
        8) Checks if BeforeFilterBy works as expected for window functions.
        9) Deletes created connection and dataset.
    """
    # uploading file, creating connection and dataset
    index_column_name = INDEX_COLUMN
    base_columns_to_select = VALUE_COLUMNS
    pandas_df = read_superstore_csv_as_pandas_df(index_column_name, base_columns_to_select)

    # prepare PG data
    upload_data_from_df(pandas_df, integration_tests_postgres_1)

    # create connection and dataset
    bi_api_client = BIAPIClient.create_client(
        base_url=ext_sys_requisites.DATALENS_API_LB_MAIN_BASE_URL,
        folder_id=integration_tests_folder_id,
        account_creds=two_users_configuration.user_1,
        logger=integration_tests_reporter,
        tenant=tenant,
    )
    main_request_executor = RequestExecutor(bi_api_client)

    response = await setup_db_conn_and_dataset(
        setup_executor=main_request_executor,
        connection_settings=integration_tests_postgres_1,
        base_dir=BASE_CLOUD_DIRECTORY,
        test_dataset=datasets.PG_SALES,
        workbook_id=workbook_id,
        admin_user_ids=[],
    )

    csv_dataset_id = response.json["id"]
    connection_id = response.json["dataset"]["sources"][0]["connection_id"]

    # updating dataset with window-function-based fields and converting date to DataLens format
    field_to_formula = {
        "Group Sales": "SUM([Sales])",
        "Rank of Sales": 'RANK([Group Sales], "asc" TOTAL)',
        "Unique Rank of Sales": 'RANK_UNIQUE([Group Sales], "asc" TOTAL)',
        "Dense Rank of Region Sales": "RANK_DENSE(SUM([Group Sales] WITHIN [Region]))",
        "Total Sales": "SUM([Group Sales] TOTAL)",
        "Date Sales": "SUM([Group Sales] WITHIN [Order Date])",
        "Region Sales": "SUM([Group Sales] WITHIN [Region])",
        "Sales Sum for Tech Operations in Region": 'SUM_IF([Group Sales], [Category] = "Technology" WITHIN [Region])',
        "Total RSUM": "RSUM([Group Sales] TOTAL ORDER BY [Row ID])",
        "Total RSUM With BFB": "RSUM([Group Sales] TOTAL ORDER BY [Row ID] BEFORE FILTER BY [Row ID])",
        "Average Sales for Region": "AVG([Group Sales] WITHIN [Region])",
        "Average Furniture Sales for Region": 'AVG_IF([Group Sales], [Category] = "Furniture" WITHIN [Region])',
        "First Sales Sum for Region": "FIRST([Group Sales] WITHIN [Region] ORDER BY [Row ID])",
        "Last Sales Sum for Category": "LAST([Group Sales] WITHIN [Category] ORDER BY [Row ID])",
        "Lag-7 Sales Sum for Category": "LAG([Group Sales], 7 WITHIN [Category] ORDER BY [Row ID])",
        "MA-3 Sales Sum": "MAVG([Group Sales], 2 ORDER BY [Row ID])",
    }
    await add_formula_fields_to_dataset(main_request_executor, csv_dataset_id, field_to_formula)

    # comparing DataLens window function results with Pandas ones
    ds_response = await bi_api_client.execute_request(Req("get", f"/api/v1/datasets/{csv_dataset_id}/versions/draft/"))

    required_fields_from_datalens = [index_column_name] + base_columns_to_select + list(field_to_formula.keys())
    # we select required base columns and window-based ones
    datalens_df = await _create_df_from_datalens_response(
        bi_api_client=bi_api_client,
        ds_response=ds_response,
        field_titles=required_fields_from_datalens,
    )
    datalens_df = datalens_df.set_index(index_column_name, verify_integrity=True).sort_index()

    # checking that base data was imported successfully from both sources
    assert_frame_equal(pandas_df, datalens_df[base_columns_to_select])

    cur_column_to_compare = "Group Sales"
    pandas_df[cur_column_to_compare] = pandas_df["Sales"].copy()
    assert_series_equal(pandas_df[cur_column_to_compare], datalens_df[cur_column_to_compare])

    cur_column_to_compare = "Rank of Sales"
    pandas_df[cur_column_to_compare] = pandas_df["Group Sales"].rank(method="min").astype(int)
    assert_series_equal(pandas_df[cur_column_to_compare], datalens_df[cur_column_to_compare])

    # DataLens UNIQUE_RANK function has non-determined value of rank for values in the same group
    cur_column_to_compare = "Unique Rank of Sales"
    pandas_unique_rank = pandas_df["Group Sales"].rank(method="first").astype(int)
    datalens_unique_rank = datalens_df[cur_column_to_compare]
    expected_unique_rank = list(range(1, len(pandas_df.index) + 1))
    # Check global correctness of the created rank
    assert sorted(pandas_unique_rank) == sorted(datalens_unique_rank) == expected_unique_rank

    # Check that local discrepancies were created in an expected way
    sales_to_pandas_rank, sales_to_datalens_rank = defaultdict(list), defaultdict(list)
    for i in expected_unique_rank:
        if pandas_unique_rank[i] != datalens_unique_rank[i]:
            sales_to_pandas_rank[pandas_df["Group Sales"][i]].append(pandas_unique_rank[i])
            sales_to_datalens_rank[datalens_df["Group Sales"][i]].append(datalens_unique_rank[i])

    for sales_key in sales_to_pandas_rank:
        assert sorted(sales_to_pandas_rank[sales_key]) == sorted(sales_to_datalens_rank[sales_key])

    cur_column_to_compare = "Total Sales"
    pandas_df[cur_column_to_compare] = pandas_df["Group Sales"].sum()
    assert_series_equal(pandas_df[cur_column_to_compare], datalens_df[cur_column_to_compare])

    cur_column_to_compare = "Date Sales"
    pandas_df[cur_column_to_compare] = pandas_df.groupby("Order Date")["Group Sales"].transform(sum)
    assert_series_equal(pandas_df[cur_column_to_compare], datalens_df[cur_column_to_compare])

    cur_column_to_compare = "Region Sales"
    pandas_df[cur_column_to_compare] = pandas_df.groupby("Region")["Group Sales"].transform(sum)
    assert_series_equal(pandas_df[cur_column_to_compare], datalens_df[cur_column_to_compare])

    cur_column_to_compare = "Sales Sum for Tech Operations in Region"
    sum_technology_sales = pandas_df.loc[pandas_df["Category"] == "Technology"].groupby("Region")["Group Sales"].sum()
    pandas_df[cur_column_to_compare] = pandas_df["Region"].map(sum_technology_sales)
    assert_series_equal(pandas_df[cur_column_to_compare], datalens_df[cur_column_to_compare])

    cur_column_to_compare = "Dense Rank of Region Sales"
    pandas_df[cur_column_to_compare] = pandas_df["Region Sales"].rank(method="dense", ascending=False).astype(int)
    assert_series_equal(pandas_df[cur_column_to_compare], datalens_df[cur_column_to_compare])

    cur_column_to_compare = "Total RSUM"
    pandas_df[cur_column_to_compare] = pandas_df["Group Sales"].expanding().sum()
    assert_series_equal(pandas_df[cur_column_to_compare], datalens_df[cur_column_to_compare])

    cur_column_to_compare = "Average Sales for Region"
    pandas_df[cur_column_to_compare] = pandas_df.groupby("Region")["Group Sales"].transform("mean")
    assert_series_equal(pandas_df[cur_column_to_compare], datalens_df[cur_column_to_compare])

    cur_column_to_compare = "Average Furniture Sales for Region"
    avg_furniture_sales = pandas_df.loc[pandas_df["Category"] == "Furniture"].groupby("Region")["Group Sales"].mean()
    pandas_df[cur_column_to_compare] = pandas_df["Region"].map(avg_furniture_sales)
    assert_series_equal(pandas_df[cur_column_to_compare], datalens_df[cur_column_to_compare])

    cur_column_to_compare = "First Sales Sum for Region"
    pandas_df[cur_column_to_compare] = pandas_df.groupby("Region")["Group Sales"].transform("first")
    assert_series_equal(pandas_df[cur_column_to_compare], datalens_df[cur_column_to_compare])

    cur_column_to_compare = "Last Sales Sum for Category"
    pandas_df[cur_column_to_compare] = pandas_df.groupby("Category")["Group Sales"].transform("last")
    assert_series_equal(pandas_df[cur_column_to_compare], datalens_df[cur_column_to_compare])

    cur_column_to_compare = "Lag-7 Sales Sum for Category"
    pandas_df[cur_column_to_compare] = pandas_df.groupby("Category")["Group Sales"].transform("shift", 7)
    assert_series_equal(pandas_df[cur_column_to_compare], datalens_df[cur_column_to_compare])

    cur_column_to_compare = "MA-3 Sales Sum"
    pandas_df[cur_column_to_compare] = pandas_df["Group Sales"].rolling(3, min_periods=1).mean()
    assert_series_equal(pandas_df[cur_column_to_compare], datalens_df[cur_column_to_compare])

    start_index_for_filter = 2000
    where_condition_json = {
        "column": guids_from_titles(ds_response.json["dataset"]["result_schema"], ["Row ID"])[0],
        "operation": "GT",
        "values": [start_index_for_filter],
    }

    datalens_df = await _create_df_from_datalens_response(
        bi_api_client=bi_api_client,
        ds_response=ds_response,
        field_titles=["Row ID", "Total RSUM", "Total RSUM With BFB"],
        where_conditions=[where_condition_json],
    )
    datalens_df = datalens_df.set_index(index_column_name, verify_integrity=True).sort_index()

    pandas_total_rsum_filtered = (
        pandas_df.loc[pandas_df.index > start_index_for_filter]["Group Sales"].expanding().sum()
    )
    assert_series_equal(pandas_total_rsum_filtered, datalens_df["Total RSUM"], check_names=False)

    pandas_total_rsum_filtered_with_bfb = (
        pandas_df["Group Sales"].expanding().sum().loc[pandas_df.index > start_index_for_filter]
    )
    assert_series_equal(pandas_total_rsum_filtered_with_bfb, datalens_df["Total RSUM With BFB"], check_names=False)

    await bi_api_client.execute_request(Req("delete", f"/api/v1/datasets/{csv_dataset_id}"))
    await bi_api_client.execute_request(Req("delete", f"/api/v1/connections/{connection_id}"))
