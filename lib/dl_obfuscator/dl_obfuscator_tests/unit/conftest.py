import pytest

from dl_obfuscator import (
    ObfuscationEngine,
    SecretKeeper,
    SecretObfuscator,
)


@pytest.fixture
def secret_keeper() -> SecretKeeper:
    secret_keeper = SecretKeeper()
    secret_keeper.add_secret("master_token", "abc123def456")
    secret_keeper.add_secret("api_key", "sk-1234567890abcdef")
    secret_keeper.add_param("user_filter", "user_id=12345")
    secret_keeper.add_param("custom_param", "sensitive_value")
    return secret_keeper


@pytest.fixture
def secret_obfuscator(secret_keeper: SecretKeeper) -> SecretObfuscator:
    obfuscator = SecretObfuscator(secret_keeper)
    return obfuscator


@pytest.fixture
def engine(secret_obfuscator: SecretObfuscator) -> ObfuscationEngine:
    engine = ObfuscationEngine()
    engine.add_obfuscator(secret_obfuscator)
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
