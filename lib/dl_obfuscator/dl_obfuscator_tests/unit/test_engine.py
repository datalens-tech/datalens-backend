from dl_obfuscator import (
    ObfuscationContext,
    ObfuscationEngine,
    SecretKeeper,
)


class TestObfuscationEngine:
    """Test cases for ObfuscationEngine"""

    def test_init(self, secret_keeper: SecretKeeper) -> None:
        """Test engine initialization"""
        engine = ObfuscationEngine(secret_keeper)
        assert engine.secret_keeper == secret_keeper
        assert len(engine._obfuscators) == 0

    def test_obfuscate_text_secrets(self, secret_keeper: SecretKeeper) -> None:
        engine = ObfuscationEngine(secret_keeper)

        text = "Token: abc123def456 and API key: sk-1234567890abcdef"

        obfuscated_logs = engine.obfuscate_text(text, ObfuscationContext.LOGS)
        obfuscated_inspector = engine.obfuscate_text(text, ObfuscationContext.INSPECTOR)

        assert "abc123def456" not in obfuscated_logs
        assert "sk-1234567890abcdef" not in obfuscated_logs
        assert "***master_token***" in obfuscated_logs
        assert "***api_key***" in obfuscated_logs

        assert "abc123def456" not in obfuscated_inspector
        assert "sk-1234567890abcdef" not in obfuscated_inspector

    def test_obfuscate_text_query_params(self, secret_keeper: SecretKeeper) -> None:
        engine = ObfuscationEngine(secret_keeper)

        text = "Filter: user_id=12345 and param: sensitive_value"

        obfuscated_logs = engine.obfuscate_text(text, ObfuscationContext.LOGS)
        obfuscated_inspector = engine.obfuscate_text(text, ObfuscationContext.INSPECTOR)

        assert "user_id=12345" not in obfuscated_logs
        assert "sensitive_value" not in obfuscated_logs

        assert "user_id=12345" in obfuscated_inspector
        assert "sensitive_value" in obfuscated_inspector

    def test_obfuscate_empty_or_none(self, secret_keeper: SecretKeeper) -> None:
        engine = ObfuscationEngine(secret_keeper)

        assert engine.obfuscate("", ObfuscationContext.LOGS) == ""
        assert engine.obfuscate(None, ObfuscationContext.LOGS) is None
        assert engine.obfuscate(123, ObfuscationContext.LOGS) == 123

    def test_obfuscate_dict(self, secret_keeper: SecretKeeper, sample_dict: dict) -> None:
        engine = ObfuscationEngine(secret_keeper)

        obfuscated = engine.obfuscate_dict(sample_dict, ObfuscationContext.LOGS)

        assert "abc123def456" not in str(obfuscated)
        assert "sk-1234567890abcdef" not in str(obfuscated)
        assert "user_id=12345" not in str(obfuscated)

        # Check structure is preserved
        assert "message" in obfuscated
        assert "nested" in obfuscated
        assert "data" in obfuscated["nested"]

    def test_obfuscate_dict_inspector_context(self, secret_keeper: SecretKeeper, sample_dict: dict) -> None:
        engine = ObfuscationEngine(secret_keeper)

        obfuscated = engine.obfuscate_dict(sample_dict, ObfuscationContext.INSPECTOR)

        assert "abc123def456" not in str(obfuscated)
        assert "sk-1234567890abcdef" not in str(obfuscated)

        assert "user_id=12345" in str(obfuscated)

    def test_obfuscate_generic(self, secret_keeper: SecretKeeper) -> None:
        engine = ObfuscationEngine(secret_keeper)

        text = "Token: abc123def456"
        obfuscated = engine.obfuscate(text, ObfuscationContext.LOGS)
        assert "abc123def456" not in obfuscated

        data = {"token": "abc123def456"}
        obfuscated = engine.obfuscate(data, ObfuscationContext.LOGS)
        assert "abc123def456" not in str(obfuscated)

    def test_context_behavior(self, secret_keeper: SecretKeeper) -> None:
        engine = ObfuscationEngine(secret_keeper)

        text = "Secret: abc123def456, Query: user_id=12345"

        contexts = [
            ObfuscationContext.LOGS,
            ObfuscationContext.SENTRY,
            ObfuscationContext.TRACING,
            ObfuscationContext.USAGE_TRACKING,
        ]

        for context in contexts:
            obfuscated = engine.obfuscate_text(text, context)
            assert "abc123def456" not in obfuscated
            assert "user_id=12345" not in obfuscated

        inspector_obfuscated = engine.obfuscate_text(text, ObfuscationContext.INSPECTOR)
        assert "abc123def456" not in inspector_obfuscated
        assert "user_id=12345" in inspector_obfuscated
