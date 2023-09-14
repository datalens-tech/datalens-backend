from bi_formula.translation.env import (
    FunctionStatsSignature,
    TranslationStats,
)


def test_function_stats_combine():
    stats_1 = TranslationStats(
        cache_hits=2,
        function_usage_weights={
            FunctionStatsSignature(name="my_func", arg_types=("int",), dialect="qqqq", is_window=False): 1,
            FunctionStatsSignature(name="my_func", arg_types=("int", "str"), dialect="qqqq", is_window=False): 3,
        },
    )
    stats_2 = TranslationStats(
        cache_hits=5,
        function_usage_weights={
            FunctionStatsSignature(name="my_func", arg_types=("int", "str"), dialect="qqqq", is_window=False): 4,
            FunctionStatsSignature(name="other_func", arg_types=(), dialect="qqqq", is_window=False): 10,
        },
    )
    expected_combined_stats = TranslationStats(
        cache_hits=7,
        function_usage_weights={
            FunctionStatsSignature(name="my_func", arg_types=("int",), dialect="qqqq", is_window=False): 1,
            FunctionStatsSignature(name="my_func", arg_types=("int", "str"), dialect="qqqq", is_window=False): 7,
            FunctionStatsSignature(name="other_func", arg_types=(), dialect="qqqq", is_window=False): 10,
        },
    )
    assert TranslationStats.combine(stats_1, stats_2) == expected_combined_stats
    assert stats_1 + stats_2 == expected_combined_stats
