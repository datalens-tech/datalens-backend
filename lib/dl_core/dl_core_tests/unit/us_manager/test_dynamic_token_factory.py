import threading
import time

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
import jwt

import dl_core.united_storage_client as united_storage_client
import dl_core.us_manager.dynamic_token_factory as dynamic_token_factory


_private_key_obj = rsa.generate_private_key(public_exponent=65537, key_size=2048)
TEST_PRIVATE_KEY = _private_key_obj.private_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption(),
).decode()
TEST_PUBLIC_KEY = (
    _private_key_obj.public_key()
    .public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    .decode()
)


def test_generates_valid_jwt():
    factory = dynamic_token_factory.DynamicUSMasterTokenFactory(
        private_key=TEST_PRIVATE_KEY,
        token_lifetime_sec=3600,
        min_ttl_sec=900.0,
    )
    ctx = factory.get_auth_context(us_master_token=None)
    assert isinstance(ctx, united_storage_client.USAuthContextPrivateOSS)
    payload = jwt.decode(ctx.us_dynamic_master_token, TEST_PUBLIC_KEY, algorithms=["RS256"])
    assert payload["serviceId"] == "bi"
    assert "iat" in payload
    assert "exp" in payload
    assert payload["exp"] - payload["iat"] == 3600


def test_caches_token():
    factory = dynamic_token_factory.DynamicUSMasterTokenFactory(
        private_key=TEST_PRIVATE_KEY,
        token_lifetime_sec=3600,
        min_ttl_sec=900.0,
    )
    ctx1 = factory.get_auth_context(us_master_token=None)
    ctx2 = factory.get_auth_context(us_master_token=None)
    assert ctx1.us_dynamic_master_token == ctx2.us_dynamic_master_token


def test_refreshes_when_close_to_expiry():
    factory = dynamic_token_factory.DynamicUSMasterTokenFactory(
        private_key=TEST_PRIVATE_KEY,
        token_lifetime_sec=2,
        min_ttl_sec=1.5,
    )
    ctx1 = factory.get_auth_context(us_master_token=None)
    time.sleep(1)
    ctx2 = factory.get_auth_context(us_master_token=None)
    assert ctx1.us_dynamic_master_token != ctx2.us_dynamic_master_token


def test_passes_master_token_when_provided():
    factory = dynamic_token_factory.DynamicUSMasterTokenFactory(
        private_key=TEST_PRIVATE_KEY,
        token_lifetime_sec=3600,
        min_ttl_sec=900.0,
    )
    ctx = factory.get_auth_context(us_master_token="static-token")
    assert ctx.us_master_token == "static-token"
    assert ctx.us_dynamic_master_token is not None


def test_no_master_token_when_none():
    factory = dynamic_token_factory.DynamicUSMasterTokenFactory(
        private_key=TEST_PRIVATE_KEY,
        token_lifetime_sec=3600,
        min_ttl_sec=900.0,
    )
    ctx = factory.get_auth_context(us_master_token=None)
    assert ctx.us_master_token is None


def test_thread_safety():
    factory = dynamic_token_factory.DynamicUSMasterTokenFactory(
        private_key=TEST_PRIVATE_KEY,
        token_lifetime_sec=3600,
        min_ttl_sec=900.0,
    )
    results: list[str] = []
    errors: list[Exception] = []

    def get_token():
        try:
            ctx = factory.get_auth_context(us_master_token=None)
            results.append(ctx.us_dynamic_master_token)
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=get_token) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    assert len(results) == 10
    assert len(set(results)) == 1


from dl_core.united_storage_client import USAuthContextMaster
from dl_core.us_manager.factory import USMFactory
import dl_retrier


def test_usm_factory_uses_dynamic_token_when_key_set():
    dynamic_factory = dynamic_token_factory.DynamicUSMasterTokenFactory(
        private_key=TEST_PRIVATE_KEY,
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
    payload = jwt.decode(ctx.us_dynamic_master_token, TEST_PUBLIC_KEY, algorithms=["RS256"])
    assert payload["serviceId"] == "bi"


def test_usm_factory_no_master_token_when_disabled():
    dynamic_factory = dynamic_token_factory.DynamicUSMasterTokenFactory(
        private_key=TEST_PRIVATE_KEY,
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


def test_usm_factory_falls_back_when_no_dynamic_factory():
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
