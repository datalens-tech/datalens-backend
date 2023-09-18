import tempfile

from dl_formula.core.datatype import DataType
from dl_formula_ref.examples.data_table import (
    DataColumn,
    DataTable,
)
from dl_formula_ref.examples.result_storage import (
    ReadableDataStorage,
    StorageKey,
    WritableDataStorage,
)


def test_result_storage():
    original_data_table = DataTable(
        columns=[
            DataColumn(name="int_value", data_type=DataType.INTEGER),
            DataColumn(name="str_value", data_type=DataType.STRING),
        ],
        rows=[
            [11, "qwe"],
            [45, "rty"],
        ],
    )
    storage_key = StorageKey(
        item_key="smth",
        example_key="whatever",
    )

    with tempfile.NamedTemporaryFile() as st_file:
        st_filename = st_file.name
        w_storage = WritableDataStorage(filename=st_filename)
        r_storage = ReadableDataStorage(filename=st_filename)

        w_storage.set_result_data(storage_key=storage_key, result_data=original_data_table)
        w_storage.save()
        r_storage.load()
        loaded_data_table = r_storage.get_result_data(storage_key=storage_key)

    assert loaded_data_table == original_data_table
