import pytest

from dl_api_lib_tests.db.base import DefaultApiTestBase
from dl_api_lib_tests.db.data_api.result.complex_queries.generation.generator import (
    AutoGeneratorSettings,
    LODTestAutoGenerator,
    TestSettings,
)
from dl_api_lib_tests.db.data_api.result.complex_queries.generation.runner import PreGeneratedLODTestRunner
from dl_constants.enums import WhereClauseOperation


GENERATED_TESTS = [
    {
        "base_dimensions": ["category"],
        "measure_formulas": [
            "SUM(AVG(AVG(SUM(AVG(SUM(AVG([sales] INCLUDE [region]) INCLUDE [ship_date]) INCLUDE [city]) INCLUDE [sub_category]) INCLUDE [order_date]) INCLUDE [ship_mode] BEFORE FILTER BY [category]))",  # noqa
            "AVG(SUM(AVG(SUM(SUM([sales] INCLUDE [region]) INCLUDE [city]) INCLUDE [sub_category]) INCLUDE [ship_mode]))",
        ],
        "filters": {"category": {"op": "EQ", "values": ["Furniture"]}},
    },
    {
        "base_dimensions": ["sub_category", "city"],
        "measure_formulas": [
            "SUM(SUM([sales] INCLUDE [region]) BEFORE FILTER BY [profit])",
            "AVG(SUM(AVG(AVG([sales] INCLUDE [region]) INCLUDE [order_date]) INCLUDE [ship_mode]))",
            "SUM([sales])",
            "AVG(SUM(AVG([sales] INCLUDE [order_date]) INCLUDE [category]) BEFORE FILTER BY [profit])",
        ],
        "filters": {"profit": {"op": "GT", "values": ["1.0"]}},
    },
    {
        "base_dimensions": ["region"],
        "measure_formulas": [
            "SUM(SUM(SUM(SUM([sales] INCLUDE [category]) INCLUDE [city] BEFORE FILTER BY [profit]) INCLUDE [ship_date]))",
            "AVG([sales])",
        ],
        "filters": {"profit": {"op": "GT", "values": ["1.0"]}},
    },
    {
        "base_dimensions": ["region"],
        "measure_formulas": [
            "AVG(AVG(AVG(AVG([sales] INCLUDE [category]) INCLUDE [ship_mode]) INCLUDE [sub_category]))",
            "SUM(AVG(AVG(SUM(AVG(SUM(SUM([sales] INCLUDE [order_date]) INCLUDE [city]) INCLUDE [sub_category]) INCLUDE [ship_date]) INCLUDE [category]) INCLUDE [ship_mode] BEFORE FILTER BY [order_date]))",  # noqa
            "SUM(AVG([sales] INCLUDE [city]) BEFORE FILTER BY [order_date])",
        ],
        "filters": {"order_date": {"op": "GT", "values": ["2014-03-01"]}},
    },
    {
        "base_dimensions": ["category"],
        "measure_formulas": [
            "SUM(AVG([sales] INCLUDE [ship_mode] BEFORE FILTER BY [category]))",
            "SUM(SUM(SUM(SUM(SUM(AVG(SUM([sales] INCLUDE [ship_mode] BEFORE FILTER BY [category]) INCLUDE [region]) INCLUDE [order_date]) INCLUDE [city]) INCLUDE [ship_date]) INCLUDE [sub_category]))",  # noqa
            "AVG(AVG(SUM([sales] INCLUDE [ship_mode]) INCLUDE [region] BEFORE FILTER BY [category]))",
        ],
        "filters": {"category": {"op": "EQ", "values": ["Furniture"]}},
    },
    {
        "base_dimensions": ["region"],
        "measure_formulas": [
            "SUM(SUM(AVG(SUM(SUM(AVG(AVG([sales] INCLUDE [order_date]) INCLUDE [sub_category]) INCLUDE [city]) INCLUDE [ship_mode]) INCLUDE [ship_date]) INCLUDE [category]) BEFORE FILTER BY [category])",  # noqa
            "SUM(AVG(AVG(AVG(SUM(AVG([sales] INCLUDE [ship_mode]) INCLUDE [city]) INCLUDE [sub_category]) INCLUDE [ship_date]) INCLUDE [category]))",
            "SUM(AVG(AVG(SUM(SUM([sales] INCLUDE [ship_mode]) INCLUDE [city] BEFORE FILTER BY [category]) INCLUDE [ship_date]) INCLUDE [category]))",
            "AVG(AVG(SUM(AVG([sales] INCLUDE [ship_date]) INCLUDE [ship_mode] BEFORE FILTER BY [category]) INCLUDE [city]))",
        ],
        "filters": {"category": {"op": "EQ", "values": ["Furniture"]}},
    },
    {
        "base_dimensions": ["region"],
        "measure_formulas": [
            "AVG(SUM([sales] INCLUDE [city]))",
            "SUM(AVG(AVG(SUM(SUM(AVG([sales] INCLUDE [order_date]) INCLUDE [ship_date] BEFORE FILTER BY [category]) INCLUDE [city]) INCLUDE [category]) INCLUDE [sub_category]))",
            "AVG(AVG(SUM(AVG(SUM([sales] INCLUDE [order_date]) INCLUDE [ship_date]) INCLUDE [category]) INCLUDE [ship_mode]) BEFORE FILTER BY [category])",
            "SUM(SUM([sales] INCLUDE [order_date]))",
        ],
        "filters": {"category": {"op": "EQ", "values": ["Furniture"]}},
    },
]


class TestPreGeneratedComplexQueryTests(DefaultApiTestBase):
    @pytest.mark.parametrize("raw_test_settings", GENERATED_TESTS)
    def test_pre_generated(self, control_api, data_api, dataset_id, raw_test_settings):
        test_runner = PreGeneratedLODTestRunner(control_api=control_api, data_api=data_api, dataset_id=dataset_id)
        test_runner.run_test(
            test_settings=TestSettings.deserialize(raw_test_settings),
        )


class TestNewGeneratedComplexQueryTests(DefaultApiTestBase):
    @pytest.mark.skip  # Should only be used for generating new tests
    def test_new_auto_generated(self, control_api, data_api, dataset_id):
        autogen_settings = AutoGeneratorSettings(
            dimensions=(
                "category",
                "city",
                "order_date",
                "region",
                "ship_date",
                "ship_mode",
                "sub_category",
            ),
            dates=frozenset(("order_date", "ship_date")),
            filters={
                "city": (WhereClauseOperation.STARTSWITH, ["New"]),
                "order_date": (WhereClauseOperation.GT, ["2014-03-01"]),
                "profit": (WhereClauseOperation.GT, ["1.0"]),
                "category": (WhereClauseOperation.EQ, ["Furniture"]),
            },
            dimension_cnts=(1, 2),
            aggregations=("AVG", "SUM"),
            formula_cnts=(2, 3, 4),
            measure_base_expressions=("[sales]",),
            filter_probability=0.1,
            bfb_probability=0.2,
            lookup_probability=0.1,
        )
        auto_gen = LODTestAutoGenerator(settings=autogen_settings)
        setting_list = auto_gen.generate_setting_list(100)

        test_runner = PreGeneratedLODTestRunner(control_api=control_api, data_api=data_api, dataset_id=dataset_id)
        test_runner.run_test_list(setting_list, ignore_400_error=False)
