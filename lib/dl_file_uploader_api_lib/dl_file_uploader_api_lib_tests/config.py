import attr

from dl_configs.crypto_keys import (
    CryptoKeysConfig,
    get_single_key_crypto_keys_config,
)


CONNECTOR_WHITELIST = ["clickhouse", "file", "gsheets_v2"]


@attr.s(kw_only=True)
class TestingUSConfig:
    container_name: str = attr.ib(default="bi_file_uploader_api_us-tests")
    master_token: str = attr.ib(default="AC1ofiek8coB")
    base_url: str = attr.ib()
    crypto_keys_config: CryptoKeysConfig = attr.ib(
        default=get_single_key_crypto_keys_config(
            key_id="0",
            key_value="BRa9gGXGm9qyy1efiIH7OkRHALjAohYtxOz-wSvlARY=",
        )
    )
    psycopg2_pg_dns: str = attr.ib()
    force_clear_db_on_launch: bool = attr.ib(default=True)
