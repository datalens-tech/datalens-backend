import os
from typing import ClassVar, Optional

import attr

from dl_configs.crypto_keys import get_single_key_crypto_keys_config, CryptoKeysConfig


# TODO FIX: Move to testing & reuse in all other modules
from dl_testing.containers import get_test_container_hostport


@attr.s(auto_attribs=True, kw_only=True)
class TestingUSConfig:
    base_url: str
    crypto_keys_config: CryptoKeysConfig
    master_token: str
    psycopg2_pg_dns: str
    force_clear_db_on_launch: bool


# TODO FIX: Move to testing & reuse in all other modules
#  Most part of this class should be moved to bi-testing.
#  Things like port numbers/tokens/container names should be declared in base class as class vars.
#  It will be used be used in config normalization methods (like `us_config` property).
#  This values should be defined in subclass for each project.
class DockerComposeEnv:
    US_MASTER_TOKEN: ClassVar[str]
    US_DB_NAME: ClassVar[str]
    US_DB_USERNAME: ClassVar[str]
    US_DB_PASSWORD: ClassVar[str]
    # Essentially flipping the default to 'on'.
    # use `CLEAR_US_DATABASE=` to disable it
    US_FORCE_CLEAR_DATABASE: ClassVar[bool] = os.environ.get('CLEAR_US_DATABASE', '1') not in ('0', '')

    CRYPTO_KEYS_CONFIG: ClassVar[CryptoKeysConfig]

    HOST_US_HTTP: ClassVar[int]
    PORT_US_HTTP: ClassVar[int]
    HOST_US_PG_5432: ClassVar[int]
    PORT_US_PG_5432: ClassVar[int]

    DB_PG_HOST: ClassVar[str]
    DB_PG_PORT: ClassVar[int]
    DB_PG_DB_NAME: ClassVar[str]
    DB_PG_USERNAME: ClassVar[str]
    DB_PG_PASSWORD: ClassVar[str]

    EXT_QUERY_EXECUTOR_SECRET_KEY: ClassVar[str]

    IAM_HOST: ClassVar[Optional[str]] = ""
    IAM_PORT: ClassVar[Optional[int]] = -1

    @property
    def us_config(self) -> TestingUSConfig:
        return TestingUSConfig(
            base_url=f"http://{self.HOST_US_HTTP}:{self.PORT_US_HTTP}",
            psycopg2_pg_dns=(
                f'host={self.HOST_US_PG_5432}'
                f' port={self.PORT_US_PG_5432}'
                f' user={self.US_DB_USERNAME}'
                f' password={self.US_DB_PASSWORD}'
                f' dbname={self.US_DB_NAME}'
            ),
            crypto_keys_config=self.CRYPTO_KEYS_CONFIG,
            master_token=self.US_MASTER_TOKEN,
            force_clear_db_on_launch=self.US_FORCE_CLEAR_DATABASE,
        )


class DockerComposeEnvBiExtApi(DockerComposeEnv):
    US_MASTER_TOKEN = 'AC1ofiek8coB'
    US_DB_NAME = "us-db-ci_purgeable"
    US_DB_USERNAME = "us"
    US_DB_PASSWORD = "us"

    EXT_QUERY_EXECUTOR_SECRET_KEY = '_some_test_secret_key_'

    CRYPTO_KEYS_CONFIG = get_single_key_crypto_keys_config(
        key_id="0",
        key_value="BRa9gGXGm9qyy1efiIH7OkRHALjAohYtxOz-wSvlARY=",
    )

    PORT_US_HTTP = get_test_container_hostport('us', fallback_port=51000).port
    HOST_US_HTTP = get_test_container_hostport('us', fallback_port=51000).host
    PORT_US_PG_5432 = get_test_container_hostport('pg-us', fallback_port=51009).port
    HOST_US_PG_5432 = get_test_container_hostport('pg-us', fallback_port=51009).host

    # Testing database configs
    DB_PG_HOST = get_test_container_hostport('db-postgres', fallback_port=51013).host
    DB_PG_PORT = get_test_container_hostport('db-postgres', fallback_port=51013).port
    DB_PG_DB_NAME = "datalens"
    DB_PG_USERNAME = "datalens"
    DB_PG_PASSWORD = "qwerty"


class DockerComposeEnvBiExtApiDC(DockerComposeEnvBiExtApi):
    PORT_US_HTTP = get_test_container_hostport('us-dc', fallback_port=51001).port
    HOST_US_HTTP = get_test_container_hostport('us-dc', fallback_port=51001).host
    PORT_US_PG_5432 = get_test_container_hostport('pg-us-dc', fallback_port=51010).port
    HOST_US_PG_5432 = get_test_container_hostport('pg-us-dc', fallback_port=51010).host

    IAM_PORT = 51042
    IAM_HOST = "0.0.0.0"
