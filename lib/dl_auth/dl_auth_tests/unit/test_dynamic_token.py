import threading

import jwt
import pytest

import dl_auth.dynamic_token as dynamic_token


def test_generates_valid_jwt(private_key: str, public_key: str) -> None:
    generator = dynamic_token.DynamicMasterTokenGenerator(
        private_key=private_key,
        token_lifetime_sec=3600,
        min_ttl_sec=900.0,
    )
    token = generator.get_token()
    payload = jwt.decode(token, public_key, algorithms=["RS256"])
    assert payload["serviceId"] == "bi"
    assert "iat" in payload
    assert "exp" in payload
    assert payload["exp"] - payload["iat"] == 3600


def test_caches_token(private_key: str) -> None:
    generator = dynamic_token.DynamicMasterTokenGenerator(
        private_key=private_key,
        token_lifetime_sec=3600,
        min_ttl_sec=900.0,
    )
    assert generator.get_token() == generator.get_token()


def test_refreshes_when_close_to_expiry(
    private_key: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeClock:
        def __init__(self, t: float) -> None:
            self.t = t

        def monotonic(self) -> float:
            return self.t

        def time(self) -> float:
            return self.t

    fake = _FakeClock(1000.0)
    monkeypatch.setattr(dynamic_token, "time", fake)

    generator = dynamic_token.DynamicMasterTokenGenerator(
        private_key=private_key,
        token_lifetime_sec=2,
        min_ttl_sec=0.5,
    )
    token_1 = generator.get_token()
    fake.t += 1.6
    token_2 = generator.get_token()
    assert token_1 != token_2


def test_thread_safety(private_key: str) -> None:
    generator = dynamic_token.DynamicMasterTokenGenerator(
        private_key=private_key,
        token_lifetime_sec=3600,
        min_ttl_sec=900.0,
    )
    results: list[str] = []
    errors: list[Exception] = []

    def get_token() -> None:
        try:
            results.append(generator.get_token())
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=get_token) for _ in range(10)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert not errors
    assert len(results) == 10
    assert len(set(results)) == 1
