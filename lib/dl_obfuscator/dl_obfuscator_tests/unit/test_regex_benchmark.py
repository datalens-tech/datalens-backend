import re
import time

import flaky
import pytest

from dl_obfuscator.obfuscators.regex import RegexObfuscator


def _build_clean_text() -> str:
    """~500 char log line with no secrets — the hot path."""
    return (
        "2026-02-19 12:34:56.789 INFO [request_id=abc-def-123] "
        "Processing dataset query for user 42. "
        "SELECT col1, col2, col3 FROM schema.table_name "
        "WHERE created_at > '2026-01-01' AND status = 'active' "
        "ORDER BY created_at DESC LIMIT 100 OFFSET 0. "
        "Query completed in 0.234s, returned 87 rows. "
        "Cache miss, storing result with TTL=300s. "
        "Response size: 12.4KB, compression ratio: 0.65. "
        "Downstream latency: db=180ms, cache_write=12ms, serialization=42ms."
    )


def _build_sparse_text() -> str:
    """Realistic log line with 1-2 embedded tokens."""
    jwt = "eyJhbGciOiJSUzI1NiI.eyJpc3MiOiJrdWJlcm5ldGVz.signature_value_here_long_enough"
    return (
        f"2026-02-19 12:34:56.789 WARN [request_id=abc-def-123] "
        f"Authentication failed for token={jwt}. "
        f"Retrying with refresh. User agent: python-requests/2.28.1"
    )


def _build_dense_text() -> str:
    """Pathological case with multiple different token types."""
    jwt = "eyJhbGciOiJSUzI1NiI.eyJpc3MiOiJrdWJlcm5ldGVz.signature_value_here_long_enough"
    url = "postgresql://admin:XXXXXXXXXXXXXXX@db.example.com:5432/mydb"
    basic = "Basic " + "d" * 24
    bearer = "Bearer " + "x" * 40
    oauth = "OAuth " + "y" * 40
    return f"auth={jwt} dsn={url} cred={basic} tok={bearer} oa={oauth}"


ITERATIONS = 10_000


def _bench_combined(patterns: tuple[str, ...], text: str, iterations: int) -> float:
    combined = re.compile("|".join(f"(?:{p})" for p in patterns))
    start = time.perf_counter()
    for _ in range(iterations):
        combined.sub("***", text)
    return time.perf_counter() - start


def _bench_sequential(patterns: tuple[str, ...], text: str, iterations: int) -> float:
    compiled = tuple(re.compile(p) for p in patterns)
    start = time.perf_counter()
    for _ in range(iterations):
        t = text
        for pat in compiled:
            t = pat.sub("***", t)
    return time.perf_counter() - start


class TestRegexBenchmark:
    @flaky.flaky(max_runs=3)
    @pytest.mark.parametrize(
        "scenario_name, text_factory",
        [
            ("clean_text", _build_clean_text),
            ("sparse_matches", _build_sparse_text),
        ],
    )
    def test_sequential_faster_than_combined(
        self,
        scenario_name: str,
        text_factory: object,
    ) -> None:
        """Sequential is faster for the hot path: clean text and sparse matches."""
        text = text_factory()
        patterns = RegexObfuscator.DEFAULT_PATTERNS

        combined_time = _bench_combined(patterns, text, ITERATIONS)
        sequential_time = _bench_sequential(patterns, text, ITERATIONS)

        speedup = combined_time / sequential_time
        print(
            f"\n  {scenario_name}: "
            f"sequential={sequential_time:.4f}s, "
            f"combined={combined_time:.4f}s, "
            f"speedup={speedup:.2f}x"
        )

        assert sequential_time < combined_time, (
            f"Sequential regex ({sequential_time:.4f}s) was not faster than "
            f"combined ({combined_time:.4f}s) for {scenario_name}"
        )

    def test_dense_matches_comparison(self) -> None:
        """Dense matches (pathological case) — informational, no assertion on winner."""
        text = _build_dense_text()
        patterns = RegexObfuscator.DEFAULT_PATTERNS

        combined_time = _bench_combined(patterns, text, ITERATIONS)
        sequential_time = _bench_sequential(patterns, text, ITERATIONS)

        print(
            f"\n  dense_matches: "
            f"sequential={sequential_time:.4f}s, "
            f"combined={combined_time:.4f}s, "
            f"ratio={sequential_time / combined_time:.2f}"
        )
