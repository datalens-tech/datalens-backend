import datetime

from pytest import MonkeyPatch

import dl_api_commons


class FakeDatetime:
    def __init__(self, intervals: list[int]) -> None:
        self.intervals = intervals
        self.position = 0

    def now(self) -> datetime.datetime:
        if self.position >= len(self.intervals):
            return datetime.datetime.fromtimestamp(self.intervals[-1])

        self.position += 1
        return datetime.datetime.fromtimestamp(self.intervals[self.position - 1])


class FakeDatetimeModule:
    def __init__(self, intervals: list[int]) -> None:
        self.timedelta = datetime.timedelta
        self.datetime = FakeDatetime(intervals)


def test_iter_retries_zero_timeout(monkeypatch: MonkeyPatch) -> None:
    fake_datetime = FakeDatetimeModule(
        [
            0,  # enter
            0,  # 1 call
            1,  # 2 call
        ]
    )
    monkeypatch.setattr(dl_api_commons.retrier.policy, "datetime", fake_datetime)

    policy = dl_api_commons.RetryPolicy(
        total_timeout=0,
        connect_timeout=1,
        request_timeout=1,
        retries_count=5,
        retryable_codes=frozenset([404, 403, 500]),
        backoff_initial=1,
        backoff_factor=2,
        backoff_max=90,
    )

    retries = list(policy.iter_retries())
    expected = [
        dl_api_commons.Retry(
            request_timeout=0,
            connect_timeout=0,
            sleep_before_seconds=0,
        ),
    ]

    assert retries == expected


def test_iter_retries_multiple(monkeypatch: MonkeyPatch) -> None:
    fake_datetime = FakeDatetimeModule(
        [
            0,  # enter
            0,  # 1 call
            1,  # 2 call (sleep=0 + timeout=1)
            3,  # 3 call (sleep=1 + timeout=1)
            6,  # 4 call (sleep=2 + timeout=1)
            11,  # 5 call (sleep=4 + timeout=1)
            20,  # 6 call (sleep=8 + timeout=1)
            37,  # 7 call (sleep=16 + timeout=1)
            70,  # 7 call (sleep=32 + timeout=1)
            135,  # 7 call (sleep=64 + timeout=1)
        ]
    )
    monkeypatch.setattr(dl_api_commons.retrier.policy, "datetime", fake_datetime)

    policy = dl_api_commons.RetryPolicy(
        total_timeout=100,
        connect_timeout=1,
        request_timeout=1,
        retries_count=4,
        retryable_codes=frozenset([404, 403, 500]),
        backoff_initial=1,
        backoff_factor=2,
        backoff_max=90,
    )

    retries_iterator = policy.iter_retries()
    expected = [
        dl_api_commons.Retry(
            request_timeout=1,
            connect_timeout=1,
            sleep_before_seconds=0,
        ),
        dl_api_commons.Retry(
            request_timeout=1,
            connect_timeout=1,
            sleep_before_seconds=1,
        ),
        dl_api_commons.Retry(
            request_timeout=1,
            connect_timeout=1,
            sleep_before_seconds=2,
        ),
        dl_api_commons.Retry(
            request_timeout=1,
            connect_timeout=1,
            sleep_before_seconds=4,
        ),
        dl_api_commons.Retry(
            request_timeout=1,
            connect_timeout=1,
            sleep_before_seconds=8,
        ),
    ]

    assert list(retries_iterator) == expected


def test_iter_retries_backoff_max_limit(monkeypatch: MonkeyPatch) -> None:
    fake_datetime = FakeDatetimeModule(
        [
            0,  # 1 call
            1,  # 2 call (sleep=0 + timeout=1)
            3,  # 3 call (sleep=1 + timeout=1)
            6,  # 4 call (sleep=2 + timeout=1)
            11,  # 5 call (sleep=4 + timeout=1)
            20,  # 6 call (sleep=8 + timeout=1)
            37,  # 7 call (sleep=12 + timeout=1)
            70,  # 7 call (sleep=12 + timeout=1)
            135,  # 7 call (sleep=12 + timeout=1)
        ]
    )
    monkeypatch.setattr(dl_api_commons.retrier.policy, "datetime", fake_datetime)

    policy = dl_api_commons.RetryPolicy(
        total_timeout=100,
        connect_timeout=1,
        request_timeout=1,
        retries_count=5,
        retryable_codes=frozenset([404, 403, 500]),
        backoff_initial=1,
        backoff_factor=2,
        backoff_max=12,
    )

    retries = list(policy.iter_retries())

    expected = [
        dl_api_commons.Retry(
            request_timeout=1,
            connect_timeout=1,
            sleep_before_seconds=0,
        ),
        dl_api_commons.Retry(
            request_timeout=1,
            connect_timeout=1,
            sleep_before_seconds=1,
        ),
        dl_api_commons.Retry(
            request_timeout=1,
            connect_timeout=1,
            sleep_before_seconds=2,
        ),
        dl_api_commons.Retry(
            request_timeout=1,
            connect_timeout=1,
            sleep_before_seconds=4,
        ),
        dl_api_commons.Retry(
            request_timeout=1,
            connect_timeout=1,
            sleep_before_seconds=8,
        ),
        dl_api_commons.Retry(
            request_timeout=1,
            connect_timeout=1,
            sleep_before_seconds=12,
        ),
    ]

    assert retries == expected


def test_iter_retries_timeout_limits_request_and_connect(monkeypatch: MonkeyPatch) -> None:
    fake_datetime = FakeDatetimeModule(
        [
            0,  # enter
            0,  # 1 call
            30,  # 2 call (sleep=0 + timeout=30)
            45,  # 3 call (sleep=1 + timeout=14)
            47,  # 4 call (sleep=2 + timeout=0)
        ]
    )
    monkeypatch.setattr(dl_api_commons.retrier.policy, "datetime", fake_datetime)

    policy = dl_api_commons.RetryPolicy(
        total_timeout=45,
        connect_timeout=30,
        request_timeout=30,
        retries_count=10,
        retryable_codes=frozenset([404, 403, 500]),
        backoff_initial=1,
        backoff_factor=2,
        backoff_max=60,
    )

    retries = list(policy.iter_retries())

    expected = [
        dl_api_commons.Retry(
            request_timeout=30,
            connect_timeout=30,
            sleep_before_seconds=0,
        ),
        dl_api_commons.Retry(
            request_timeout=14,  # clipped by total time left (+ sleep=1)
            connect_timeout=14,
            sleep_before_seconds=1,
        ),
    ]

    assert retries == expected


def test_iter_retries_stops_when_sleep_exceeds_timeout(monkeypatch: MonkeyPatch) -> None:
    fake_datetime = FakeDatetimeModule(
        [
            0,  # enter
            0,  # 1 call
            1,  # 2 call (sleep=0 + timeout=1)
            3,  # 3 call (sleep=1 + timeout=1)
            6,  # 4 call (sleep=2 + timeout=1)
        ]
    )
    monkeypatch.setattr(dl_api_commons.retrier.policy, "datetime", fake_datetime)

    policy = dl_api_commons.RetryPolicy(
        total_timeout=4,
        connect_timeout=1,
        request_timeout=1,
        retries_count=5,
        retryable_codes=frozenset([500]),
        backoff_initial=1,
        backoff_factor=2,
        backoff_max=60,
    )

    retries = list(policy.iter_retries())

    expected = [
        dl_api_commons.Retry(
            request_timeout=1,
            connect_timeout=1,
            sleep_before_seconds=0,
        ),
        dl_api_commons.Retry(
            request_timeout=1,
            connect_timeout=1,
            sleep_before_seconds=1,
        ),
        # Next iteration will require sleep_before_seconds2, but only 1 second left
    ]

    assert retries == expected


def test_iter_retries_zero_retries_count(monkeypatch: MonkeyPatch) -> None:
    fake_datetime = FakeDatetimeModule(
        [
            0,  # enter
            0,  # 1 call
            5,  # 2 call (sleep=0 + timeout=5)
        ]
    )
    monkeypatch.setattr(dl_api_commons.retrier.policy, "datetime", fake_datetime)

    policy = dl_api_commons.RetryPolicy(
        total_timeout=10,
        connect_timeout=5,
        request_timeout=5,
        retries_count=0,  # No retries
        retryable_codes=frozenset([500]),
        backoff_initial=1,
        backoff_factor=2,
        backoff_max=60,
    )

    retries = list(policy.iter_retries())

    # Should get at least single attempt
    expected = [
        dl_api_commons.Retry(
            request_timeout=5,
            connect_timeout=5,
            sleep_before_seconds=0,
        ),
    ]

    assert retries == expected


def test_iter_retries_one_retry_count(monkeypatch: MonkeyPatch) -> None:
    fake_datetime = FakeDatetimeModule(
        [
            0,  # enter
            0,  # 1 call
            5,  # 2 call (sleep=0 + timeout=5)
            17,  # 2 call (sleep=2 + timeout=10)
        ]
    )
    monkeypatch.setattr(dl_api_commons.retrier.policy, "datetime", fake_datetime)

    policy = dl_api_commons.RetryPolicy(
        total_timeout=100,
        connect_timeout=5,
        request_timeout=5,
        retries_count=1,
        retryable_codes=frozenset([500]),
        backoff_initial=2,
        backoff_factor=3,
        backoff_max=60,
    )

    retries = list(policy.iter_retries())

    expected = [
        dl_api_commons.Retry(
            request_timeout=5,
            connect_timeout=5,
            sleep_before_seconds=0,
        ),
        dl_api_commons.Retry(
            request_timeout=5,
            connect_timeout=5,
            sleep_before_seconds=2,
        ),
    ]

    assert retries == expected
