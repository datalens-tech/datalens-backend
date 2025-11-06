import ssl

import pytest
import pytest_mock

import dl_auth
import dl_retrier
import dl_testing


@pytest.fixture(name="ssl_context")
def fixture_ssl_context() -> ssl.SSLContext:
    return dl_testing.get_default_ssl_context()


@pytest.fixture(name="mock_retry")
def fixture_mock_retry() -> dl_retrier.Retry:
    return dl_retrier.Retry(
        request_timeout=10,
        connect_timeout=30,
        sleep_before_seconds=0,
    )


@pytest.fixture(name="mock_retry_policy")
def fixture_mock_retry_policy(
    mocker: pytest_mock.MockerFixture,
    mock_retry: dl_retrier.Retry,
) -> dl_retrier.RetryPolicy:
    retry_policy = mocker.Mock(spec=dl_retrier.RetryPolicy)
    retry_policy.iter_retries.return_value = iter([mock_retry, mock_retry, mock_retry])
    retry_policy.can_retry_error.return_value = False
    return retry_policy


@pytest.fixture(name="mock_retry_policy_factory")
def fixture_mock_retry_policy_factory(
    mocker: pytest_mock.MockerFixture,
    mock_retry_policy: dl_retrier.RetryPolicy,
) -> dl_retrier.RetryPolicyFactory:
    retry_policy_factory = mocker.MagicMock(spec=dl_retrier.RetryPolicyFactory)
    retry_policy_factory.get_policy.return_value = mock_retry_policy

    return retry_policy_factory


@pytest.fixture(name="mock_auth_provider")
def fixture_mock_auth_provider(
    mocker: pytest_mock.MockerFixture,
) -> dl_auth.AuthProviderProtocol:
    auth_provider = mocker.MagicMock(spec=dl_auth.AuthProviderProtocol)
    return auth_provider
