import pytest

from dl_obfuscator import (
    create_base_obfuscators,
    create_request_engine,
)
from dl_obfuscator.context import ObfuscationContext
from dl_obfuscator.obfuscators.regex import (
    DEFAULT_PATTERNS,
    RegexObfuscator,
)


_EXTRA_PATTERNS_FOR_TEST: tuple[str, ...] = (
    r"XTOKEN_[A-Za-z0-9]{20,}",
    r"test-key-[0-9a-f]{16}",
)


def test_default_patterns() -> None:
    obfuscator = RegexObfuscator()
    assert obfuscator.patterns == DEFAULT_PATTERNS
    assert len(obfuscator._compiled_patterns) == len(DEFAULT_PATTERNS)


def test_custom_patterns() -> None:
    custom = (r"secret_\d+",)
    obfuscator = RegexObfuscator(patterns=custom)
    assert obfuscator.patterns == custom


def test_custom_replacement() -> None:
    obfuscator = RegexObfuscator(
        patterns=(r"secret_\d+",),
        replacement="[REDACTED]",
    )
    result = obfuscator.obfuscate("has secret_123 here", ObfuscationContext.LOGS)
    assert result == "has [REDACTED] here"


def test_clean_text_unchanged() -> None:
    obfuscator = RegexObfuscator()
    text = "SELECT * FROM table WHERE id = 42 AND name = 'hello world'"
    assert obfuscator.obfuscate(text, ObfuscationContext.LOGS) == text


def test_empty_string() -> None:
    obfuscator = RegexObfuscator()
    assert obfuscator.obfuscate("", ObfuscationContext.LOGS) == ""


def test_jwt() -> None:
    obfuscator = RegexObfuscator()
    token = "eyJhbGciOiJSUzI1NiI.eyJpc3MiOiJrdWJlcm.signature_value_here_long"
    text = f"Authorization: Bearer {token}"
    result = obfuscator.obfuscate(text, ObfuscationContext.LOGS)
    assert "eyJhbGci" not in result


def test_url_credentials() -> None:
    obfuscator = RegexObfuscator()
    url = "postgresql://admin:XXXXXXXXXXXXXXX@db.example.com:5432/mydb"
    text = f"connecting to {url}"
    result = obfuscator.obfuscate(text, ObfuscationContext.LOGS)
    assert "XXXXXXXXXXXXXXX" not in result


def test_authorization_header_bearer() -> None:
    obfuscator = RegexObfuscator()
    text = "Authorization: Bearer some-long-token-value"
    result = obfuscator.obfuscate(text, ObfuscationContext.LOGS)
    assert "some-long-token-value" not in result


def test_authorization_header_basic() -> None:
    obfuscator = RegexObfuscator()
    text = "Authorization: Basic dXNlcjpwYXNzd29yZA=="
    result = obfuscator.obfuscate(text, ObfuscationContext.LOGS)
    assert "dXNlcjpwYXNzd29yZA==" not in result


def test_basic_auth_value_without_header() -> None:
    obfuscator = RegexObfuscator()
    text = "credentials: Basic dXNlcjpwYXNzd29yZA=="
    result = obfuscator.obfuscate(text, ObfuscationContext.LOGS)
    assert "dXNlcjpwYXNzd29yZA==" not in result


def test_bearer_token_without_header() -> None:
    obfuscator = RegexObfuscator()
    text = "token is Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9_long_value"
    result = obfuscator.obfuscate(text, ObfuscationContext.LOGS)
    assert "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9_long_value" not in result


def test_oauth_token_value() -> None:
    obfuscator = RegexObfuscator()
    text = "token is OAuth some_long_oauth_token_value_1234567890"
    result = obfuscator.obfuscate(text, ObfuscationContext.LOGS)
    assert "some_long_oauth_token_value_1234567890" not in result


@pytest.mark.parametrize(
    "keyword",
    ["basic", "Basic", "BASIC", "bAsIc"],
)
def test_basic_case_insensitive(keyword: str) -> None:
    obfuscator = RegexObfuscator()
    text = f"credentials: {keyword} dXNlcjpwYXNzd29yZA=="
    result = obfuscator.obfuscate(text, ObfuscationContext.LOGS)
    assert "dXNlcjpwYXNzd29yZA==" not in result


def test_extra_patterns_combined_with_defaults() -> None:
    extra = _EXTRA_PATTERNS_FOR_TEST
    obfuscator = RegexObfuscator(patterns=DEFAULT_PATTERNS + extra)

    token = "XTOKEN_" + "a" * 25
    text = f"key={token}"
    result = obfuscator.obfuscate(text, ObfuscationContext.LOGS)
    assert token not in result


def test_factory_default_uses_only_public_patterns() -> None:
    obfuscators = create_base_obfuscators()
    regex_obfuscators = [o for o in obfuscators if isinstance(o, RegexObfuscator)]
    assert len(regex_obfuscators) == 1
    assert regex_obfuscators[0].patterns == DEFAULT_PATTERNS


def test_factory_with_extra_patterns() -> None:
    obfuscators = create_base_obfuscators(extra_regex_patterns=_EXTRA_PATTERNS_FOR_TEST)
    regex_obfuscators = [o for o in obfuscators if isinstance(o, RegexObfuscator)]
    assert len(regex_obfuscators) == 1
    expected_count = len(DEFAULT_PATTERNS) + len(_EXTRA_PATTERNS_FOR_TEST)
    assert len(regex_obfuscators[0].patterns) == expected_count


def test_factory_extra_patterns_work_in_engine() -> None:
    obfuscators = create_base_obfuscators(extra_regex_patterns=_EXTRA_PATTERNS_FOR_TEST)
    engine = create_request_engine(base_obfuscators=obfuscators)

    xtoken = "XTOKEN_" + "a" * 25
    test_key = "test-key-" + "a" * 16
    jwt = "eyJhbGciOiJSUzI1NiI.eyJpc3MiOiJrdWJlcm.signature_value_here_long"

    text = f"x={xtoken} k={test_key} jwt={jwt}"
    result = engine.obfuscate(text, ObfuscationContext.LOGS)

    assert xtoken not in result  # extra pattern
    assert test_key not in result  # extra pattern
    assert "eyJhbGci" not in result  # default pattern
