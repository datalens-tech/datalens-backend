import ssl

import pytest

import dl_core_testing
import dl_httpx
import dl_retrier
import dl_testing
import dl_us_entries_client


@pytest.fixture(name="us_host_port")
def fixture_us_host_port() -> dl_testing.HostPort:
    return dl_testing.get_test_container_hostport("us")


@pytest.fixture(name="us_pg_host_port")
def fixture_us_pg_host_port() -> dl_testing.HostPort:
    return dl_testing.get_test_container_hostport("pg-us")


@pytest.fixture(name="prepared_us")
def fixture_prepared_us(
    us_host_port: dl_testing.HostPort,
    us_pg_host_port: dl_testing.HostPort,
) -> None:
    us_config = dl_core_testing.UnitedStorageConfiguration(
        us_master_token="AC1ofiek8coB",
        us_host=f"http://{us_host_port.as_pair()}",
        us_pg_dsn=f"host={us_pg_host_port.host} port={us_pg_host_port.port} user=us password=us dbname=us-db-ci_purgeable",
    )
    dl_core_testing.prepare_united_storage_from_config(us_config)


@pytest.fixture(name="ssl_context")
def fixture_ssl_context() -> ssl.SSLContext:
    return dl_testing.get_default_ssl_context()


@pytest.fixture(name="us_entries_client")
def fixture_us_entries_client(
    prepared_us: None,
    us_host_port: dl_testing.HostPort,
    ssl_context: ssl.SSLContext,
) -> dl_us_entries_client.USEntriesAsyncClient:
    return dl_us_entries_client.USEntriesAsyncClient.from_dependencies(
        dependencies=dl_httpx.HttpxClientDependencies(
            base_url=f"http://{us_host_port.as_pair()}",
            ssl_context=ssl_context,
            retry_policy_factory=dl_retrier.RetryPolicyFactory.from_settings(
                settings=dl_retrier.RetryPolicyFactorySettings(
                    DEFAULT_POLICY=dl_retrier.RetryPolicySettings(
                        TOTAL_TIMEOUT=5,
                    ),
                ),
            ),
        )
    )
