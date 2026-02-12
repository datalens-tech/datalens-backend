import pytest

from dl_obfuscator import (
    ObfuscationEngine,
    SecretKeeper,
    SecretObfuscator,
)


@pytest.fixture
def secret_keeper() -> SecretKeeper:
    secret_keeper = SecretKeeper()
    secret_keeper.add_secret("abc123def456", "master_token")
    secret_keeper.add_secret("sk-1234567890abcdef", "api_key")
    secret_keeper.add_param("user_id=12345", "user_filter")
    secret_keeper.add_param("sensitive_value", "sensitive_param")
    return secret_keeper


@pytest.fixture
def secret_obfuscator(secret_keeper: SecretKeeper) -> SecretObfuscator:
    obfuscator = SecretObfuscator(keeper=secret_keeper)
    return obfuscator


@pytest.fixture
def engine(secret_obfuscator: SecretObfuscator) -> ObfuscationEngine:
    engine = ObfuscationEngine()
    engine.add_base_obfuscator(secret_obfuscator)
    return engine


@pytest.fixture
def sample_text() -> str:
    return "Processing request with token: abc123def456 and filter: user_id=12345"


@pytest.fixture
def sample_dict() -> dict:
    return {
        "message": "Processing request with token: abc123def456",
        "query_params": {"user_filter": "user_id=12345"},
        "api_key": "sk-1234567890abcdef",
        "nested": {
            "token": "abc123def456",
            "data": {
                "more_data": "user_id=12345",
                "other_data": "other_data",
            },
        },
    }
