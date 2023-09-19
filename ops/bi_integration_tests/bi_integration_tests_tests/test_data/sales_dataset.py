import io
import pkgutil

import pandas as pd


INDEX_COLUMN = "Row ID"
VALUE_COLUMNS = ["Region", "Category", "Sales", "Order Date"]

SOURCE_CSV_NAME = "superstore_sales_sample_dataset.csv"
TEST_FILE_API_FILENAME = "test.csv"


def read_superstore_csv_as_pandas_df(index_column_name=INDEX_COLUMN, base_columns_to_select=None):
    if base_columns_to_select is None:
        base_columns_to_select = VALUE_COLUMNS
    csv_file_binary = pkgutil.get_data(__package__, "/".join([SOURCE_CSV_NAME]))
    df = pd.read_csv(io.BytesIO(csv_file_binary), encoding="utf8", sep=",", index_col=index_column_name)
    df = df[base_columns_to_select].sort_index()
    pandas_df = df
    return pandas_df


def get_test_csv_file_contents():
    return pkgutil.get_data(__package__, "/".join([TEST_FILE_API_FILENAME])).decode("utf8")
