import asyncio
import time

import flaky
import pytest

from dl_obfuscator import (
    ObfuscationContext,
    ObfuscationEngine,
    SecretKeeper,
    SecretObfuscator,
    create_base_obfuscators,
    create_request_engine,
)
from dl_obfuscator.engine import ObfuscatableData


class TestObfuscationEngine:
    def test_init(self, secret_keeper: SecretKeeper) -> None:
        obfuscator = SecretObfuscator(keeper=secret_keeper)
        engine = ObfuscationEngine()
        assert len(engine._base_obfuscators) == 0
        engine.add_base_obfuscator(obfuscator)
        assert len(engine._base_obfuscators) == 1

    def test_obfuscate_text_secrets(self, engine: ObfuscationEngine) -> None:
        text = "Token: abc123def456 and API key: sk-1234567890abcdef"

        obfuscated_logs = engine.obfuscate(text, ObfuscationContext.LOGS)
        obfuscated_inspector = engine.obfuscate(text, ObfuscationContext.INSPECTOR)

        assert "abc123def456" not in obfuscated_logs
        assert "sk-1234567890abcdef" not in obfuscated_logs
        assert "***master_token***" in obfuscated_logs
        assert "***api_key***" in obfuscated_logs

        assert "abc123def456" not in obfuscated_inspector
        assert "sk-1234567890abcdef" not in obfuscated_inspector

    def test_obfuscate_text_query_params(self, engine: ObfuscationEngine) -> None:
        text = "Filter: user_id=12345 and param: sensitive_value"

        obfuscated_logs = engine.obfuscate(text, ObfuscationContext.LOGS)
        obfuscated_inspector = engine.obfuscate(text, ObfuscationContext.INSPECTOR)

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
        num_strings = 500_000
        tokens = [ch * 3 for ch in "abcdefghijklmnopqrstuvwxyz "]
        text = "".join(ch * 3 for ch in "the quick brown fox jumps over the lazy dog")

        keeper = SecretKeeper()
        for token in tokens:
            keeper.add_secret(token, "*")

        engine = ObfuscationEngine()
        engine.add_base_obfuscator(SecretObfuscator(keeper=keeper))

        start = time.process_time()
        count = num_strings
        while count:
            text_to_obfuscate = text
            text_to_obfuscate = engine.obfuscate(text_to_obfuscate, ObfuscationContext.LOGS)
            count -= 1
        elapsed = time.process_time() - start

        assert text_to_obfuscate.replace("*", "") == ""
        assert elapsed < 10
        print(elapsed)


class TestObfuscateTypes:
    def test_obfuscate_dict(self, engine: ObfuscationEngine) -> None:
        data: dict[str, ObfuscatableData] = {
            "message": "Token: abc123def456",
            "nested": {"inner": "key sk-1234567890abcdef"},
        }
        result = engine.obfuscate(data, ObfuscationContext.LOGS)
        assert result == {
            "message": "Token: ***master_token***",
            "nested": {"inner": "key ***api_key***"},
        }

    def test_obfuscate_list(self, engine: ObfuscationEngine) -> None:
        data: list[ObfuscatableData] = ["token: abc123def456", "safe text"]
        result = engine.obfuscate(data, ObfuscationContext.LOGS)
        assert result == ["token: ***master_token***", "safe text"]

    def test_obfuscate_none(self, engine: ObfuscationEngine) -> None:
        assert engine.obfuscate(None, ObfuscationContext.LOGS) is None

    def test_obfuscate_dict_with_none_values(self, engine: ObfuscationEngine) -> None:
        data: dict[str, ObfuscatableData] = {"key": "abc123def456", "empty": None}
        result = engine.obfuscate(data, ObfuscationContext.LOGS)
        assert result == {"key": "***master_token***", "empty": None}

    def test_obfuscate_unsupported_type_raises(self, engine: ObfuscationEngine) -> None:
        with pytest.raises(TypeError, match="Cannot obfuscate type int"):
            engine.obfuscate(42, ObfuscationContext.LOGS)  # type: ignore

    def test_obfuscate_mixed_nested(self, engine: ObfuscationEngine) -> None:
        data: dict[str, ObfuscatableData] = {
            "items": ["abc123def456", "clean"],
            "inner": {"key": "sk-1234567890abcdef"},
            "nothing": None,
        }
        result = engine.obfuscate(data, ObfuscationContext.LOGS)
        assert result == {
            "items": ["***master_token***", "clean"],
            "inner": {"key": "***api_key***"},
            "nothing": None,
        }


class TestContextIsolation:
    @pytest.mark.asyncio
    async def test_concurrent_request_isolation(self) -> None:
        """Verify concurrent async tasks have isolated request obfuscators."""
        global_keeper = SecretKeeper()
        global_keeper.add_secret("GLOBAL_SECRET", "global")
        base_obfuscators = create_base_obfuscators(global_keeper=global_keeper)

        results: dict[str, str] = {}
        all_ready = asyncio.Event()
        ready_count = 0

        text = "GLOBAL_SECRET SECRET_A SECRET_B SECRET_C other_text"

        async def simulate_request(request_id: str, secret: str) -> None:
            nonlocal ready_count
            keeper = SecretKeeper()
            keeper.add_secret(secret, f"req_{request_id}")
            engine = create_request_engine(
                base_obfuscators=base_obfuscators,
                secret_keeper=keeper,
            )

            ready_count += 1
            if ready_count == 3:
                all_ready.set()
            await all_ready.wait()

            results[request_id] = engine.obfuscate(text, ObfuscationContext.LOGS)

        await asyncio.gather(
            simulate_request("A", "SECRET_A"),
            simulate_request("B", "SECRET_B"),
            simulate_request("C", "SECRET_C"),
        )

        for rid, secret in [("A", "SECRET_A"), ("B", "SECRET_B"), ("C", "SECRET_C")]:
            assert secret not in results[rid], f"Request {rid} did not obfuscate its own secret"
            assert "GLOBAL_SECRET" not in results[rid], f"Request {rid} did not obfuscate global secret"

        # Request A must NOT have obfuscated B's or C's secrets
        assert "***req_B***" not in results["A"]
        assert "***req_C***" not in results["A"]

    @pytest.mark.asyncio
    async def test_cleanup_does_not_affect_other_requests(self) -> None:
        """Verify per-request engines are fully isolated — one engine's lifecycle doesn't affect another."""
        base_obfuscators = create_base_obfuscators()

        both_ready = asyncio.Event()
        ready_count = 0
        results: dict[str, str] = {}

        async def early_request() -> None:
            nonlocal ready_count
            keeper = SecretKeeper()
            keeper.add_secret("EARLY_SECRET", "early")
            _engine = create_request_engine(
                base_obfuscators=base_obfuscators,
                secret_keeper=keeper,
            )

            ready_count += 1
            if ready_count == 2:
                both_ready.set()
            await both_ready.wait()

        async def late_request() -> None:
            nonlocal ready_count
            keeper = SecretKeeper()
            keeper.add_secret("LATE_SECRET", "late")
            engine = create_request_engine(
                base_obfuscators=base_obfuscators,
                secret_keeper=keeper,
            )

            ready_count += 1
            if ready_count == 2:
                both_ready.set()
            await both_ready.wait()

            await asyncio.sleep(0.1)  # let early request finish first
            results["late"] = engine.obfuscate("LATE_SECRET and EARLY_SECRET here", ObfuscationContext.LOGS)

        await asyncio.gather(early_request(), late_request())
        assert results["late"] == "***late*** and EARLY_SECRET here"
