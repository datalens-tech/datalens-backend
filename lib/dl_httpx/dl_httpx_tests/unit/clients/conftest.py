import ssl

import pytest

import dl_testing


@pytest.fixture(name="ssl_context")
def fixture_ssl_context() -> ssl.SSLContext:
    return dl_testing.get_default_ssl_context()
