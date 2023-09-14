from bi_formula_ref.examples.config import ExampleConfig
from bi_formula_ref.examples.data_table import (
    DataColumn,
    DataTable,
)
from bi_formula_ref.examples.result_storage import StorageKey


def make_key_for_example(func_name: str, example: ExampleConfig) -> StorageKey:
    return StorageKey(item_key=func_name, example_key=example.name)


def make_data_table_from_example(example: ExampleConfig) -> DataTable:
    return DataTable(
        columns=[DataColumn(name=name, data_type=data_type) for name, data_type in example.source.columns],
        rows=example.source.data,
    )
