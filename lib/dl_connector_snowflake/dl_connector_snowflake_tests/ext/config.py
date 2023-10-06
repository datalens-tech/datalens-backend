from dl_api_lib_testing.configuration import ApiTestEnvironmentConfiguration
from dl_constants.enums import UserDataType
from dl_core_testing.configuration import DefaultCoreTestConfiguration
from dl_testing.containers import get_test_container_hostport


# Infra settings
CORE_TEST_CONFIG = DefaultCoreTestConfiguration(
    host_us_http=get_test_container_hostport("us", fallback_port=51700).host,
    port_us_http=get_test_container_hostport("us", fallback_port=51700).port,
    host_us_pg=get_test_container_hostport("pg-us", fallback_port=51709).host,
    port_us_pg_5432=get_test_container_hostport("pg-us", fallback_port=51709).port,
    us_master_token="AC1ofiek8coB",
    core_connector_ep_names=["snowflake"],
)

# Connector settings
DB_DSN = "snowflake://does@not/matter"
# todo: maybe support also certificates and store them in sec storage, to avoid refresh token updates

SAMPLE_TABLE_SIMPLIFIED_SCHEMA = [
    ("Category", UserDataType.string),
    ("City", UserDataType.string),
    ("Country", UserDataType.string),
    ("Customer ID", UserDataType.string),
    ("Customer Name", UserDataType.string),
    ("Discount", UserDataType.float),
    ("Order Date", UserDataType.date),
    ("Order ID", UserDataType.string),
    ("Postal Code", UserDataType.integer),
    ("Product ID", UserDataType.string),
    ("Product Name", UserDataType.string),
    ("Profit", UserDataType.float),
    ("Quantity", UserDataType.integer),
    ("Region", UserDataType.string),
    ("Row ID", UserDataType.integer),
    ("Sales", UserDataType.float),
    ("Segment", UserDataType.string),
    ("Ship Date", UserDataType.date),
    ("Ship Mode", UserDataType.string),
    ("State", UserDataType.string),
    ("Sub-Category", UserDataType.string),
]


API_TEST_CONFIG = ApiTestEnvironmentConfiguration(
    api_connector_ep_names=["snowflake"],
    core_test_config=CORE_TEST_CONFIG,
    ext_query_executer_secret_key="_some_test_secret_key_",
)
