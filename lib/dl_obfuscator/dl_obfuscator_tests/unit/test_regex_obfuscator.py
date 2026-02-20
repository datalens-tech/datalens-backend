import pytest

from dl_obfuscator import (
    create_base_obfuscators,
    create_request_engine,
)
from dl_obfuscator.context import ObfuscationContext
from dl_obfuscator.obfuscators.regex import RegexObfuscator


_EXTRA_PATTERNS_FOR_TEST: tuple[str, ...] = (
    r"XTOKEN_[A-Za-z0-9]{20,}",
    r"test-key-[0-9a-f]{16}",
)


class TestRegexObfuscatorCreate:
    def test_create_default_patterns(self) -> None:
        obfuscator = RegexObfuscator.create()
        assert obfuscator.patterns == RegexObfuscator.DEFAULT_PATTERNS
        assert len(obfuscator._compiled_patterns) == len(RegexObfuscator.DEFAULT_PATTERNS)

    def test_create_custom_patterns(self) -> None:
        custom = (r"secret_\d+",)
        obfuscator = RegexObfuscator.create(patterns=custom)
        assert obfuscator.patterns == custom

    def test_create_custom_replacement(self) -> None:
        obfuscator = RegexObfuscator.create(
            patterns=(r"secret_\d+",),
            replacement="[REDACTED]",
        )
        result = obfuscator.obfuscate("has secret_123 here", ObfuscationContext.LOGS)
        assert result == "has [REDACTED] here"


class TestRegexObfuscatorNoMatch:
    def test_clean_text_unchanged(self) -> None:
        obfuscator = RegexObfuscator.create()
        text = "SELECT * FROM table WHERE id = 42 AND name = 'hello world'"
        assert obfuscator.obfuscate(text, ObfuscationContext.LOGS) == text

    def test_empty_string(self) -> None:
        obfuscator = RegexObfuscator.create()
        assert obfuscator.obfuscate("", ObfuscationContext.LOGS) == ""


class TestStandardFormatPatterns:
    @pytest.fixture()
    def obfuscator(self) -> RegexObfuscator:
        return RegexObfuscator.create()

    def test_jwt(self, obfuscator: RegexObfuscator) -> None:
        token = "eyJhbGciOiJSUzI1NiI.eyJpc3MiOiJrdWJlcm.signature_value_here_long"
        text = f"Authorization: Bearer {token}"
        result = obfuscator.obfuscate(text, ObfuscationContext.LOGS)
        assert "eyJhbGci" not in result

    def test_url_credentials(self, obfuscator: RegexObfuscator) -> None:
        url = "postgresql://admin:XXXXXXXXXXXXXXX@db.example.com:5432/mydb"
        text = f"connecting to {url}"
        result = obfuscator.obfuscate(text, ObfuscationContext.LOGS)
        assert "XXXXXXXXXXXXXXX" not in result

    def test_authorization_header_bearer(self, obfuscator: RegexObfuscator) -> None:
        text = "Authorization: Bearer some-long-token-value"
        result = obfuscator.obfuscate(text, ObfuscationContext.LOGS)
        assert "some-long-token-value" not in result

    def test_authorization_header_basic(self, obfuscator: RegexObfuscator) -> None:
        text = "Authorization: Basic dXNlcjpwYXNzd29yZA=="
        result = obfuscator.obfuscate(text, ObfuscationContext.LOGS)
        assert "dXNlcjpwYXNzd29yZA==" not in result

    def test_basic_auth_value_without_header(self, obfuscator: RegexObfuscator) -> None:
        text = "credentials: Basic dXNlcjpwYXNzd29yZA=="
        result = obfuscator.obfuscate(text, ObfuscationContext.LOGS)
        assert "dXNlcjpwYXNzd29yZA==" not in result

    def test_bearer_token_without_header(self, obfuscator: RegexObfuscator) -> None:
        text = "token is Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9_long_value"
        result = obfuscator.obfuscate(text, ObfuscationContext.LOGS)
        assert "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9_long_value" not in result

    def test_oauth_token_value(self, obfuscator: RegexObfuscator) -> None:
        text = "token is OAuth some_long_oauth_token_value_1234567890"
        result = obfuscator.obfuscate(text, ObfuscationContext.LOGS)
        assert "some_long_oauth_token_value_1234567890" not in result


class TestExtraPatternsIntegration:
    def test_extra_patterns_combined_with_defaults(self) -> None:
        extra = _EXTRA_PATTERNS_FOR_TEST
        obfuscator = RegexObfuscator.create(patterns=RegexObfuscator.DEFAULT_PATTERNS + extra)

        token = "XTOKEN_" + "a" * 25
        text = f"key={token}"
        result = obfuscator.obfuscate(text, ObfuscationContext.LOGS)
        assert token not in result

    def test_factory_default_uses_only_public_patterns(self) -> None:
        obfuscators = create_base_obfuscators()
        regex_obfuscators = [o for o in obfuscators if isinstance(o, RegexObfuscator)]
        assert len(regex_obfuscators) == 1
        assert regex_obfuscators[0].patterns == RegexObfuscator.DEFAULT_PATTERNS

    def test_factory_with_extra_patterns(self) -> None:
        obfuscators = create_base_obfuscators(extra_regex_patterns=_EXTRA_PATTERNS_FOR_TEST)
        regex_obfuscators = [o for o in obfuscators if isinstance(o, RegexObfuscator)]
        assert len(regex_obfuscators) == 1
        expected_count = len(RegexObfuscator.DEFAULT_PATTERNS) + len(_EXTRA_PATTERNS_FOR_TEST)
        assert len(regex_obfuscators[0].patterns) == expected_count

    def test_factory_extra_patterns_work_in_engine(self) -> None:
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
