from dl_constants.enums import UserDataType
from dl_core_testing.fixtures.primitives import FixtureTableSpec


TABLE_SPEC_SAMPLE_SUPERSTORE = FixtureTableSpec(
    csv_name="sample_superstore.csv",
    table_schema=(
        ("category", UserDataType.string),
        ("city", UserDataType.string),
        ("country", UserDataType.string),
        ("customer_id", UserDataType.string),
        ("customer_name", UserDataType.string),
        ("discount", UserDataType.float),
        ("order_date", UserDataType.date),
        ("order_id", UserDataType.string),
        ("postal_code", UserDataType.integer),
        ("product_id", UserDataType.string),
        ("product_name", UserDataType.string),
        ("profit", UserDataType.float),
        ("quantity", UserDataType.integer),
        ("region", UserDataType.string),
        ("row_id", UserDataType.integer),
        ("sales", UserDataType.float),
        ("segment", UserDataType.string),
        ("ship_date", UserDataType.date),
        ("ship_mode", UserDataType.string),
        ("state", UserDataType.string),
        ("sub_category", UserDataType.string),
    ),
)
