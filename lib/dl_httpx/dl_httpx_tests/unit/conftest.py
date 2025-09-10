import pytest
import pytest_mock

import dl_retrier


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
