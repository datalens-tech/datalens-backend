from dl_constants.enums import BIType
from dl_core_testing.fixtures.primitives import FixtureTableSpec

TABLE_SPEC_SAMPLE_SUPERSTORE = FixtureTableSpec(
    csv_name="sample_superstore.csv",
    table_schema=(
        ("category", BIType.string),
        ("city", BIType.string),
        ("country", BIType.string),
        ("customer_id", BIType.string),
        ("customer_name", BIType.string),
        ("discount", BIType.float),
        ("order_date", BIType.date),
        ("order_id", BIType.string),
        ("postal_code", BIType.integer),
        ("product_id", BIType.string),
        ("product_name", BIType.string),
        ("profit", BIType.float),
        ("quantity", BIType.integer),
        ("region", BIType.string),
        ("row_id", BIType.integer),
        ("sales", BIType.float),
        ("segment", BIType.string),
        ("ship_date", BIType.date),
        ("ship_mode", BIType.string),
        ("state", BIType.string),
        ("sub_category", BIType.string),
    ),
)
