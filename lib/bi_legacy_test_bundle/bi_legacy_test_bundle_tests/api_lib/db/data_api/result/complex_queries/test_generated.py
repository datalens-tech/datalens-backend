import pytest

from bi_legacy_test_bundle_tests.api_lib.db.data_api.result.complex_queries.generation.generator import (
    AutoGeneratorSettings,
    LODTestAutoGenerator,
    TestSettings,
)
from bi_legacy_test_bundle_tests.api_lib.db.data_api.result.complex_queries.generation.runner import (
    PreGeneratedLODTestRunner,
)
from dl_constants.enums import WhereClauseOperation


GENERATED_TESTS = [
    {
        "base_dimensions": ["Category"],
        "measure_formulas": [
            "SUM(AVG(AVG(SUM(AVG(SUM(AVG([Sales] INCLUDE [Region]) INCLUDE [Ship Date]) INCLUDE [City]) INCLUDE [Sub-Category]) INCLUDE [Order Date]) INCLUDE [Ship Mode] BEFORE FILTER BY [Category]))",  # noqa
            "AVG(SUM(AVG(SUM(SUM([Sales] INCLUDE [Region]) INCLUDE [City]) INCLUDE [Sub-Category]) INCLUDE [Ship Mode]))",
        ],
        "filters": {"Category": {"op": "EQ", "values": ["Furniture"]}},
    },
    {
        "base_dimensions": ["Sub-Category", "City"],
        "measure_formulas": [
            "SUM(SUM([Sales] INCLUDE [Region]) BEFORE FILTER BY [Profit])",
            "AVG(SUM(AVG(AVG([Sales] INCLUDE [Region]) INCLUDE [Order Date]) INCLUDE [Ship Mode]))",
            "SUM([Sales])",
            "AVG(SUM(AVG([Sales] INCLUDE [Order Date]) INCLUDE [Category]) BEFORE FILTER BY [Profit])",
        ],
        "filters": {"Profit": {"op": "GT", "values": ["1.0"]}},
    },
    {
        "base_dimensions": ["Region"],
        "measure_formulas": [
            "SUM(SUM(SUM(SUM([Sales] INCLUDE [Category]) INCLUDE [City] BEFORE FILTER BY [Profit]) INCLUDE [Ship Date]))",
            "AVG([Sales])",
        ],
        "filters": {"Profit": {"op": "GT", "values": ["1.0"]}},
    },
    {
        "base_dimensions": ["Region"],
        "measure_formulas": [
            "AVG(AVG(AVG(AVG([Sales] INCLUDE [Category]) INCLUDE [Ship Mode]) INCLUDE [Sub-Category]))",
            "SUM(AVG(AVG(SUM(AVG(SUM(SUM([Sales] INCLUDE [Order Date]) INCLUDE [City]) INCLUDE [Sub-Category]) INCLUDE [Ship Date]) INCLUDE [Category]) INCLUDE [Ship Mode] BEFORE FILTER BY [Order Date]))",  # noqa
            "SUM(AVG([Sales] INCLUDE [City]) BEFORE FILTER BY [Order Date])",
        ],
        "filters": {"Order Date": {"op": "GT", "values": ["2014-03-01"]}},
    },
    {
        "base_dimensions": ["Category"],
        "measure_formulas": [
            "SUM(AVG([Sales] INCLUDE [Ship Mode] BEFORE FILTER BY [Category]))",
            "SUM(SUM(SUM(SUM(SUM(AVG(SUM([Sales] INCLUDE [Ship Mode] BEFORE FILTER BY [Category]) INCLUDE [Region]) INCLUDE [Order Date]) INCLUDE [City]) INCLUDE [Ship Date]) INCLUDE [Sub-Category]))",  # noqa
            "AVG(AVG(SUM([Sales] INCLUDE [Ship Mode]) INCLUDE [Region] BEFORE FILTER BY [Category]))",
        ],
        "filters": {"Category": {"op": "EQ", "values": ["Furniture"]}},
    },
    {
        "base_dimensions": ["Region"],
        "measure_formulas": [
            "SUM(SUM(AVG(SUM(SUM(AVG(AVG([Sales] INCLUDE [Order Date]) INCLUDE [Sub-Category]) INCLUDE [City]) INCLUDE [Ship Mode]) INCLUDE [Ship Date]) INCLUDE [Category]) BEFORE FILTER BY [Category])",  # noqa
            "SUM(AVG(AVG(AVG(SUM(AVG([Sales] INCLUDE [Ship Mode]) INCLUDE [City]) INCLUDE [Sub-Category]) INCLUDE [Ship Date]) INCLUDE [Category]))",
            "SUM(AVG(AVG(SUM(SUM([Sales] INCLUDE [Ship Mode]) INCLUDE [City] BEFORE FILTER BY [Category]) INCLUDE [Ship Date]) INCLUDE [Category]))",
            "AVG(AVG(SUM(AVG([Sales] INCLUDE [Ship Date]) INCLUDE [Ship Mode] BEFORE FILTER BY [Category]) INCLUDE [City]))",
        ],
        "filters": {"Category": {"op": "EQ", "values": ["Furniture"]}},
    },
    {
        "base_dimensions": ["Region"],
        "measure_formulas": [
            "AVG(SUM([Sales] INCLUDE [City]))",
            "SUM(AVG(AVG(SUM(SUM(AVG([Sales] INCLUDE [Order Date]) INCLUDE [Ship Date] BEFORE FILTER BY [Category]) INCLUDE [City]) INCLUDE [Category]) INCLUDE [Sub-Category]))",
            "AVG(AVG(SUM(AVG(SUM([Sales] INCLUDE [Order Date]) INCLUDE [Ship Date]) INCLUDE [Category]) INCLUDE [Ship Mode]) BEFORE FILTER BY [Category])",
            "SUM(SUM([Sales] INCLUDE [Order Date]))",
        ],
        "filters": {"Category": {"op": "EQ", "values": ["Furniture"]}},
    },
]


@pytest.mark.parametrize("raw_test_settings", GENERATED_TESTS)
def test_pre_generated(api_v1, data_api_v2, dataset_id, raw_test_settings):
    test_runner = PreGeneratedLODTestRunner(control_api=api_v1, data_api=data_api_v2, dataset_id=dataset_id)
    test_runner.run_test(
        test_settings=TestSettings.deserialize(raw_test_settings),
    )


@pytest.mark.skip  # Should only be used for generating new tests
def test_new_auto_generated(api_v1, data_api_v2, dataset_id):
    autogen_settings = AutoGeneratorSettings(
        dimensions=(
            "Category",
            "City",
            "Order Date",
            "Region",
            "Ship Date",
            "Ship Mode",
            "Sub-Category",
        ),
        dates=frozenset(("Order Date", "Ship Date")),
        filters={
            "City": (WhereClauseOperation.STARTSWITH, ["New"]),
            "Order Date": (WhereClauseOperation.GT, ["2014-03-01"]),
            "Profit": (WhereClauseOperation.GT, ["1.0"]),
            "Category": (WhereClauseOperation.EQ, ["Furniture"]),
        },
        dimension_cnts=(1, 2),
        aggregations=("AVG", "SUM"),
        formula_cnts=(2, 3, 4),
        measure_base_expressions=("[Sales]",),
        filter_probability=0.1,
        bfb_probability=0.2,
        lookup_probability=0.1,
    )
    auto_gen = LODTestAutoGenerator(settings=autogen_settings)
    setting_list = auto_gen.generate_setting_list(100)

    test_runner = PreGeneratedLODTestRunner(control_api=api_v1, data_api=data_api_v2, dataset_id=dataset_id)
    test_runner.run_test_list(setting_list, ignore_400_error=False)
