from dl_api_lib_testing.configuration import BiApiTestEnvironmentConfiguration
from dl_constants.enums import BIType
from dl_core_testing.configuration import DefaultCoreTestConfiguration
from dl_testing.containers import get_test_container_hostport

# Infra settings
CORE_TEST_CONFIG = DefaultCoreTestConfiguration(
    host_us_http=get_test_container_hostport("us", fallback_port=51700).host,
    port_us_http=get_test_container_hostport("us", fallback_port=51700).port,
    host_us_pg=get_test_container_hostport("pg-us", fallback_port=51709).host,
    port_us_pg_5432=get_test_container_hostport("pg-us", fallback_port=51709).port,
    us_master_token="AC1ofiek8coB",
)

# Connector settings
DB_DSN = "snowflake://does@not/matter"
# todo: maybe support also certificates and store them in sec storage, to avoid refresh token updates

SAMPLE_TABLE_SIMPLIFIED_SCHEMA = [
    ("Category", BIType.string),
    ("City", BIType.string),
    ("Country", BIType.string),
    ("Customer ID", BIType.string),
    ("Customer Name", BIType.string),
    ("Discount", BIType.float),
    ("Order Date", BIType.date),
    ("Order ID", BIType.string),
    ("Postal Code", BIType.integer),
    ("Product ID", BIType.string),
    ("Product Name", BIType.string),
    ("Profit", BIType.float),
    ("Quantity", BIType.integer),
    ("Region", BIType.string),
    ("Row ID", BIType.integer),
    ("Sales", BIType.float),
    ("Segment", BIType.string),
    ("Ship Date", BIType.date),
    ("Ship Mode", BIType.string),
    ("State", BIType.string),
    ("Sub-Category", BIType.string),
]


BI_TEST_CONFIG = BiApiTestEnvironmentConfiguration(
    bi_api_connector_whitelist=["snowflake"],
    core_connector_whitelist=["snowflake"],
    core_test_config=CORE_TEST_CONFIG,
    ext_query_executer_secret_key="_some_test_secret_key_",
)
