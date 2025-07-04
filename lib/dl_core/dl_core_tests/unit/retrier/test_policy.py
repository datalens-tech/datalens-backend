import itertools

import dl_core.retrier.policy


def test_backoff_at():
    policy = dl_core.retrier.policy.RetryPolicy(
        total_timeout=10,
        connect_timeout=10,
        request_timeout=10,
        retries_count=10,
        retryable_codes=frozenset([404, 403, 500]),
        backoff_initial=1,
        backoff_factor=2,
        backoff_max=90,
    )

    expected = [
        1,
        2,
        4,
        8,
        16,
        32,
        64,
        90,
        90,
        90,
    ]

    for idx, expected_value in enumerate(expected):
        assert policy.get_backoff_at(idx) == expected_value


def test_backoff():
    policy = dl_core.retrier.policy.RetryPolicy(
        total_timeout=10,
        connect_timeout=10,
        request_timeout=10,
        retries_count=10,
        retryable_codes=frozenset([404, 403, 500]),
        backoff_initial=1,
        backoff_factor=2,
        backoff_max=90,
    )

    expected = [
        1,
        2,
        4,
        8,
        16,
        32,
        64,
        90,
        90,
        90,
    ]

    assert expected == list(itertools.islice(policy.get_backoff(), 10))
