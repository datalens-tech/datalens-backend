import time

import flaky

from dl_obfuscator import (
    ObfuscationContext,
    ObfuscationEngine,
    SecretKeeper,
    SecretObfuscator,
)


class TestObfuscationEngine:
    def test_init(self, secret_keeper: SecretKeeper) -> None:
        obfuscator = SecretObfuscator(secret_keeper)
        engine = ObfuscationEngine()
        assert len(engine._obfuscators) == 0
        engine.add_obfuscator(obfuscator)
        assert len(engine._obfuscators) == 1

    def test_obfuscate_text_secrets(self, engine: ObfuscationEngine) -> None:
        text = "Token: abc123def456 and API key: sk-1234567890abcdef"

        obfuscated_logs = engine.obfuscate_text(text, ObfuscationContext.LOGS)
        obfuscated_inspector = engine.obfuscate_text(text, ObfuscationContext.INSPECTOR)

        assert "abc123def456" not in obfuscated_logs
        assert "sk-1234567890abcdef" not in obfuscated_logs
        assert "***master_token***" in obfuscated_logs
        assert "***api_key***" in obfuscated_logs

        assert "abc123def456" not in obfuscated_inspector
        assert "sk-1234567890abcdef" not in obfuscated_inspector

    def test_obfuscate_text_query_params(self, engine: ObfuscationEngine) -> None:
        text = "Filter: user_id=12345 and param: sensitive_value"

        obfuscated_logs = engine.obfuscate_text(text, ObfuscationContext.LOGS)
        obfuscated_inspector = engine.obfuscate_text(text, ObfuscationContext.INSPECTOR)

        assert "user_id=12345" not in obfuscated_logs
        assert "sensitive_value" not in obfuscated_logs

        assert "user_id=12345" in obfuscated_inspector
        assert "sensitive_value" in obfuscated_inspector

    def test_context_behavior(self, engine: ObfuscationEngine) -> None:
        text = "Secret: abc123def456, Query: user_id=12345"

        contexts = [
            ObfuscationContext.LOGS,
            ObfuscationContext.SENTRY,
            ObfuscationContext.TRACING,
            ObfuscationContext.USAGE_TRACKING,
        ]

        for context in contexts:
            obfuscated = engine.obfuscate(text, context)
            assert "abc123def456" not in obfuscated
            assert "user_id=12345" not in obfuscated

        inspector_obfuscated = engine.obfuscate(text, ObfuscationContext.INSPECTOR)
        assert "abc123def456" not in inspector_obfuscated
        assert "user_id=12345" in inspector_obfuscated

    @flaky.flaky(max_runs=5)
    def test_perf(self) -> None:
        num_strings = 5_000_000
        secrets_in_a_string = "abcdefghijklmnopqrstuvwxyz"
        text = "the quick brown fox jumps over the lazy dog"

        keeper = SecretKeeper()
        for i in secrets_in_a_string:
            keeper.add_secret(i, "*")

        engine = ObfuscationEngine()
        engine.add_obfuscator(SecretObfuscator(keeper))

        start = time.perf_counter()
        count = num_strings
        while count:
            text_to_obfuscate = text
            text_to_obfuscate = engine.obfuscate(text_to_obfuscate, ObfuscationContext.LOGS)
            count -= 1
        elapsed = time.perf_counter() - start

        assert elapsed < 2.5
