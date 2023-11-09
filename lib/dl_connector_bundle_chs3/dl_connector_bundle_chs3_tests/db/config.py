from dl_api_lib_testing.configuration import ApiTestEnvironmentConfiguration
from dl_core_testing.configuration import DefaultCoreTestConfiguration
from dl_testing.containers import get_test_container_hostport

from dl_connector_bundle_chs3.chs3_base.core.settings import FileS3ConnectorSettings


CORE_TEST_CONFIG = DefaultCoreTestConfiguration(
    host_us_http=get_test_container_hostport("us", fallback_port=52611).host,
    port_us_http=get_test_container_hostport("us", fallback_port=52611).port,
    host_us_pg=get_test_container_hostport("pg-us", fallback_port=52610).host,
    port_us_pg_5432=get_test_container_hostport("pg-us", fallback_port=52610).port,
    us_master_token="AC1ofiek8coB",
    core_connector_ep_names=["clickhouse", "file", "gsheets_v2", "yadocs"],
)

SR_CONNECTION_SETTINGS = FileS3ConnectorSettings(
    SECURE=False,
    HOST=get_test_container_hostport("db-clickhouse", original_port=8123).host,
    PORT=get_test_container_hostport("db-clickhouse", original_port=8123).port,
    USERNAME="datalens",
    PASSWORD="qwerty",
    ACCESS_KEY_ID="accessKey1",
    SECRET_ACCESS_KEY="verySecretKey1",
    BUCKET="dl-file-uploader",
    S3_ENDPOINT="http://s3-storage:8000",  # compose svc name, because this is a container interaction (ch <-> s3)
)

DB_CH_URL = (
    f"clickhouse://datalens:qwerty@"
    f"{get_test_container_hostport('db-clickhouse', fallback_port=52604).as_pair()}/test_data"
)

S3_ENDPOINT_URL = f"http://{get_test_container_hostport('s3-storage', fallback_port=52620).as_pair()}"

API_TEST_CONFIG = ApiTestEnvironmentConfiguration(
    api_connector_ep_names=["clickhouse", "file", "gsheets_v2", "yadocs"],
    core_test_config=CORE_TEST_CONFIG,
    ext_query_executer_secret_key="_some_test_secret_key_",
    redis_host=get_test_container_hostport("redis", fallback_port=52604).host,
    redis_port=get_test_container_hostport("redis", fallback_port=52604).port,
    redis_password="AwockEuvavDyinmeakmiRiopanbesBepsensUrdIz5",
)
