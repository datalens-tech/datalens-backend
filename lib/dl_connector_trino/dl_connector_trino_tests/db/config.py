from typing import ClassVar

from dl_api_lib_testing.configuration import ApiTestEnvironmentConfiguration
from dl_core_testing.configuration import CoreTestEnvironmentConfiguration
from dl_testing.containers import get_test_container_hostport

from dl_connector_trino.core.constants import TrinoAuthType
from dl_connector_trino.formula.constants import TrinoDialect as D


CORE_TEST_CONFIG = CoreTestEnvironmentConfiguration(
    host_us_http=get_test_container_hostport("us", fallback_port=21133).host,
    port_us_http=get_test_container_hostport("us", fallback_port=21133).port,
    host_us_pg=get_test_container_hostport("pg-us", fallback_port=22087).host,
    port_us_pg_5432=get_test_container_hostport("pg-us", fallback_port=22087).port,
    redis_host=get_test_container_hostport("redis-caches", fallback_port=22569).host,
    redis_port=get_test_container_hostport("redis-caches", fallback_port=22569).port,
    us_master_token="AC1ofiek8coB",
    core_connector_ep_names=["trino"],
)


class CoreConnectionSettings:
    HOST: ClassVar[str] = get_test_container_hostport("trino-no-auth", fallback_port=21123).host
    PORT: ClassVar[int] = get_test_container_hostport("trino-no-auth", fallback_port=21123).port
    USERNAME: ClassVar[str] = "datalens"
    AUTH_TYPE: ClassVar[str] = TrinoAuthType.NONE


class CoreSslConnectionSettings:
    HOST: ClassVar[str] = get_test_container_hostport("trino-tls-nginx", fallback_port=21124).host
    PORT: ClassVar[int] = get_test_container_hostport("trino-tls-nginx", fallback_port=21124).port
    USERNAME: ClassVar[str] = "trino_user"
    CERT_PROVIDER_URL: ClassVar[
        str
    ] = f"http://{get_test_container_hostport('ssl-provider', fallback_port=26002).as_pair()}"


class CorePasswordConnectionSettings(CoreSslConnectionSettings):
    PASSWORD: ClassVar[str] = "trino_password"
    AUTH_TYPE: ClassVar[str] = TrinoAuthType.PASSWORD


class CoreJwtConnectionSettings(CoreSslConnectionSettings):
    JWT: ClassVar[str] = (
        "ewogICJhbGciOiAiUlMyNTYiLAogICJ0eXAiOiAiSldUIgp9Cg.ewogICJpc3MiOiAidGVzdF9qd3RfaXNzdWVyIiwKICAiYXVkI"
        "jogInRlc3RfdHJpbm9fY29vcmRpbmF0b3IiLAogICJzdWIiOiAidHJpbm9fdXNlciIsCiAgImlhdCI6IDE3MzgzMzkxOTIKfQo.k"
        "hyhr2nRZuGRG2cKCEM5vlcOxV7dh4vMSzRQ1VLa6F5-KwvytVbhgO0VvEr0FDJTs8sekU56BtzhN-bfDijQIcVdP5ePojVjrwoHs"
        "haZIPmS0fhBpR8G9XAphIToBB-WtxW3bzAqVAsmUFctUSzRw7iKilcfzS6He8YhIqGOEQ7GFQxHHiquuwo0HoH60LuDoZVPhnIRw"
        "oKkShhF7QYokB2a-9nE8OUrhvfdy4Ix_9u_FnaToXZUhTomEIOV4Fut9QihslkSlKetTsZFtD5J5oZ292wa_mI8P_jE13tMP7iZl"
        "mUhNcNenmf7WsxqSP7SZDmItS-pPdfrAWSYw4EyHw"
    )
    AUTH_TYPE: ClassVar[str] = TrinoAuthType.JWT


DB_URLS = {
    D.TRINO: f"trino://datalens@" f'{get_test_container_hostport("trino-no-auth", fallback_port=21123).as_pair()}',
    (D.TRINO, "ssl"): f"trino://trino_user@"
    f'{get_test_container_hostport("trino-tls-nginx", fallback_port=21124).as_pair()}',
}
DB_CORE_URL = DB_URLS[D.TRINO]
DB_CORE_SSL_URL = DB_URLS[(D.TRINO, "ssl")]

API_TEST_CONFIG = ApiTestEnvironmentConfiguration(
    api_connector_ep_names=["trino"],
    core_test_config=CORE_TEST_CONFIG,
    ext_query_executer_secret_key="_some_test_secret_key_",
)
