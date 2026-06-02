import threading

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
import jwt
import pytest

import dl_auth
import dl_core.united_storage_client as united_storage_client
from dl_core.united_storage_client import USAuthContextMaster
import dl_core.us_manager.dynamic_token_factory as dynamic_token_factory
from dl_core.us_manager.factory import USMFactory
import dl_retrier


@pytest.fixture(scope="module")
def test_private_key() -> str:
    key_obj = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return key_obj.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()


@pytest.fixture(scope="module")
def test_public_key(test_private_key: str) -> str:
    loaded = serialization.load_pem_private_key(test_private_key.encode(), password=None)
    return (
        loaded.public_key()
        .public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        .decode()
    )


# Tests for DynamicUSMasterTokenFactory


def test_generates_valid_jwt(test_private_key: str, test_public_key: str) -> None:
    factory = dynamic_token_factory.DynamicUSMasterTokenFactory(
        private_key=test_private_key,
        token_lifetime_sec=3600,
        min_ttl_sec=900.0,
    )
    ctx = factory.get_auth_context()
    assert isinstance(ctx, united_storage_client.USAuthContextPrivateOSS)
    payload = jwt.decode(ctx.us_dynamic_master_token, test_public_key, algorithms=["RS256"])
    assert payload["serviceId"] == "bi"
    assert "iat" in payload
    assert "exp" in payload
    assert payload["exp"] - payload["iat"] == 3600


def test_caches_token(test_private_key: str) -> None:
    factory = dynamic_token_factory.DynamicUSMasterTokenFactory(
        private_key=test_private_key,
        token_lifetime_sec=3600,
        min_ttl_sec=900.0,
    )
    ctx1 = factory.get_auth_context()
    ctx2 = factory.get_auth_context()
    assert ctx1.us_dynamic_master_token == ctx2.us_dynamic_master_token


def test_refreshes_when_close_to_expiry(test_private_key: str, monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeClock:
        def __init__(self, t: float) -> None:
            self.t = t

        def monotonic(self) -> float:
            return self.t

        def time(self) -> float:
            return self.t

    fake = _FakeClock(1000.0)
    monkeypatch.setattr(dl_auth.dynamic_token, "time", fake)

    factory = dynamic_token_factory.DynamicUSMasterTokenFactory(
        private_key=test_private_key,
        token_lifetime_sec=2,
        min_ttl_sec=0.5,
    )
    ctx1 = factory.get_auth_context()
    fake.t += 1.6
    ctx2 = factory.get_auth_context()
    assert ctx1.us_dynamic_master_token != ctx2.us_dynamic_master_token


def test_passes_master_token_when_provided(test_private_key: str) -> None:
    factory = dynamic_token_factory.DynamicUSMasterTokenFactory(
        private_key=test_private_key,
        token_lifetime_sec=3600,
        min_ttl_sec=900.0,
    )
    ctx = factory.get_auth_context(us_master_token="static-token")
    assert ctx.us_master_token == "static-token"
    assert ctx.us_dynamic_master_token is not None


def test_no_master_token_when_none(test_private_key: str) -> None:
    factory = dynamic_token_factory.DynamicUSMasterTokenFactory(
        private_key=test_private_key,
        token_lifetime_sec=3600,
        min_ttl_sec=900.0,
    )
    ctx = factory.get_auth_context()
    assert ctx.us_master_token is None


def test_thread_safety(test_private_key: str) -> None:
    factory = dynamic_token_factory.DynamicUSMasterTokenFactory(
        private_key=test_private_key,
        token_lifetime_sec=3600,
        min_ttl_sec=900.0,
    )
    results: list[str] = []
    errors: list[Exception] = []

    def get_token() -> None:
        try:
            ctx = factory.get_auth_context()
            results.append(ctx.us_dynamic_master_token)
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


# Tests for USMFactory


def test_usm_factory_uses_dynamic_token_when_key_set(test_private_key: str, test_public_key: str) -> None:
    dynamic_factory = dynamic_token_factory.DynamicUSMasterTokenFactory(
        private_key=test_private_key,
        token_lifetime_sec=3600,
        min_ttl_sec=900.0,
    )
    usm_factory = USMFactory(
        us_base_url="http://localhost:8080",
        crypto_keys_config=None,
        ca_data=b"",
        us_master_token="static-token",
        retry_policy_factory=dl_retrier.DefaultRetryPolicyFactory(),
        dynamic_token_factory=dynamic_factory,
        master_token_authorization_enabled=True,
    )
    ctx = usm_factory.get_master_auth_context_sync()
    assert isinstance(ctx, united_storage_client.USAuthContextPrivateOSS)
    assert ctx.us_master_token == "static-token"
    payload = jwt.decode(ctx.us_dynamic_master_token, test_public_key, algorithms=["RS256"])
    assert payload["serviceId"] == "bi"


def test_usm_factory_no_master_token_when_disabled(test_private_key: str) -> None:
    dynamic_factory = dynamic_token_factory.DynamicUSMasterTokenFactory(
        private_key=test_private_key,
        token_lifetime_sec=3600,
        min_ttl_sec=900.0,
    )
    usm_factory = USMFactory(
        us_base_url="http://localhost:8080",
        crypto_keys_config=None,
        ca_data=b"",
        us_master_token="static-token",
        retry_policy_factory=dl_retrier.DefaultRetryPolicyFactory(),
        dynamic_token_factory=dynamic_factory,
        master_token_authorization_enabled=False,
    )
    ctx = usm_factory.get_master_auth_context_sync()
    assert isinstance(ctx, united_storage_client.USAuthContextPrivateOSS)
    assert ctx.us_master_token is None


def test_usm_factory_falls_back_when_no_dynamic_factory() -> None:
    usm_factory = USMFactory(
        us_base_url="http://localhost:8080",
        crypto_keys_config=None,
        ca_data=b"",
        us_master_token="static-token",
        retry_policy_factory=dl_retrier.DefaultRetryPolicyFactory(),
    )
    ctx = usm_factory.get_master_auth_context_sync()
    assert isinstance(ctx, USAuthContextMaster)
    assert ctx.us_master_token == "static-token"
