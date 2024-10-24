import datetime
from http import HTTPStatus
from typing import Optional

from dl_api_client.dsmaker.primitives import WhereClause
from dl_api_client.dsmaker.shortcuts.dataset import (
    add_formulas_to_dataset,
    create_basic_dataset,
)
from dl_api_client.dsmaker.shortcuts.range_data import get_range_values
from dl_api_client.dsmaker.shortcuts.result_data import get_data_rows
from dl_api_lib_testing.api_base import DefaultApiTestBase
from dl_api_lib_testing.connector.complex_queries import DefaultBasicLookupFunctionTestSuite
from dl_api_lib_testing.helpers.data_source import data_source_settings_from_table
from dl_api_lib_testing.helpers.lookup_checkers import (
    check_ago_data,
    check_at_date_data,
    read_date,
)
from dl_constants.enums import WhereClauseOperation
from dl_core_testing.database import make_table


class TestBasicLookupFunctions(DefaultApiTestBase, DefaultBasicLookupFunctionTestSuite):
    def test_ago(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "Sales Sum": "SUM([sales])",
                "Sales Sum Yesterday": 'AGO([Sales Sum], [order_date], "day", 1)',
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="category"),
                ds.find_field(title="order_date"),
                ds.find_field(title="Sales Sum"),
                ds.find_field(title="Sales Sum Yesterday"),
            ],
            order_by=[
                ds.find_field(title="category"),
                ds.find_field(title="order_date"),
            ],
            filters=[
                ds.find_field(title="category").filter(op=WhereClauseOperation.EQ, values=["Office Supplies"]),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)
        check_ago_data(data_rows=data_rows, date_idx=1, value_idx=2, ago_idx=3, day_offset=1)

    def test_ago_variants(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "Sales Sum": "SUM([sales])",
                "Ago 2": "AGO([Sales Sum], [order_date])",
                "Ago 3 unit": 'AGO([Sales Sum], [order_date], "day")',
                "Ago 3 number": "AGO([Sales Sum], [order_date], 1)",
                "Ago 4": 'AGO([Sales Sum], [order_date], "day", 1)',
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="order_date"),
                ds.find_field(title="Sales Sum"),
                ds.find_field(title="Ago 2"),
                ds.find_field(title="Ago 3 unit"),
                ds.find_field(title="Ago 3 number"),
                ds.find_field(title="Ago 4"),
            ],
            order_by=[
                ds.find_field(title="order_date"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)
        check_ago_data(data_rows=data_rows, date_idx=0, value_idx=1, ago_idx=2, day_offset=1)
        check_ago_data(data_rows=data_rows, date_idx=0, value_idx=1, ago_idx=3, day_offset=1)
        check_ago_data(data_rows=data_rows, date_idx=0, value_idx=1, ago_idx=4, day_offset=1)
        check_ago_data(data_rows=data_rows, date_idx=0, value_idx=1, ago_idx=5, day_offset=1)

    def test_ago_errors(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "Sales Sum": "SUM([sales])",
                "Ago Sum": 'AGO([Sales Sum], [order_date], "day", 1)',
            },
        )

        # Dimension in AGO doesn't match dimensions in the query
        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="category"),
                ds.find_field(title="Sales Sum"),
                ds.find_field(title="Ago Sum"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.BAD_REQUEST, result_resp.json
        # FIXME: The more specific error for AGO is temporarily reverted to the generic inconsistent agg error
        assert result_resp.bi_status_code == "ERR.DS_API.FORMULA.VALIDATION.AGG.INCONSISTENT"
        # assert result_resp.bi_status_code == 'ERR.DS_API.FORMULA.VALIDATION.LOOKUP_FUNC.UNSELECTED_DIMENSION'

        # There are no dimensions in the query
        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="Ago Sum"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.BAD_REQUEST, result_resp.json
        # FIXME: Same as above
        assert result_resp.bi_status_code == "ERR.DS_API.FORMULA.VALIDATION.AGG.INCONSISTENT"
        # assert result_resp.bi_status_code == 'ERR.DS_API.FORMULA.VALIDATION.LOOKUP_FUNC.UNSELECTED_DIMENSION'

    def test_ago_in_compeng(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "Sales RSum": "RSUM(SUM([sales]))",
                "Ago RSum": 'AGO([Sales RSum], [order_date], "day", 1)',
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="order_date"),
                ds.find_field(title="Sales RSum"),
                ds.find_field(title="Ago RSum"),
            ],
            order_by=[
                ds.find_field(title="order_date"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)
        check_ago_data(data_rows=data_rows, date_idx=0, value_idx=1, ago_idx=2, day_offset=1)

    def test_dimensions_in_ago_identical_to_dims_in_query(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "order_date Clone": "[order_date]",
                "Group Sales": "SUM([sales])",
                "Ago Along Clone": "AGO([Group Sales], [order_date Clone])",
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="order_date"),
                ds.find_field(title="Group Sales"),
                ds.find_field(title="Ago Along Clone"),
            ],
            order_by=[
                ds.find_field(title="order_date"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)
        check_ago_data(data_rows=data_rows, date_idx=0, value_idx=1, ago_idx=2, day_offset=1)

    def test_ago_with_non_ago_aggregation(self, control_api, data_api, saved_dataset):
        """
        Check that an expression containing ago and a simple aggregation
        at the same level is sliced correctly.
        """
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "Group Sales": "SUM([sales])",
                "Ago": "AGO([Group Sales], [order_date])",
                "Ago And not Ago Agg": "[Group Sales] - [Ago]",
                # In this formula the left part ([Group Sales]) has no AGO (QueryFork) nodes in it,
                # while the right part ([Ago]) does.
                # This means that fork slicing will be used, but it needs to slice above aggregations (not below)
                # in the parts of the expression where there are no QueryFork nodes
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="order_date"),
                ds.find_field(title="Group Sales"),
                ds.find_field(title="Ago And not Ago Agg"),
            ],
            order_by=[
                ds.find_field(title="order_date"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json

    def test_ago_with_avatarless_measure(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "Avatarless Measure": "COUNT()",
                "Ago": "AGO([Avatarless Measure], [order_date])",
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="order_date"),
                ds.find_field(title="Avatarless Measure"),
                ds.find_field(title="Ago"),
            ],
            order_by=[
                ds.find_field(title="order_date"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)
        check_ago_data(data_rows=data_rows, date_idx=0, value_idx=1, ago_idx=2, day_offset=1)

    def test_ago_with_ignore_dimensions(self, control_api, data_api, saved_connection_id, db):
        db_table = make_table(db)
        ds = create_basic_dataset(
            api_v1=control_api,
            connection_id=saved_connection_id,
            data_source_settings=data_source_settings_from_table(table=db_table),
            formulas={
                "sum": "SUM([int_value])",
                # Create a dimension that would cause regular AGO to always return NULL:
                "day": "DAY([date_value])",
                # Regular AGO:
                "ago": "AGO([sum], [date_value])",
                # AGO that ignores [day] in JOIN
                "ago_igdim": "AGO([sum], [date_value] IGNORE DIMENSIONS [day])",
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="date_value"),
                ds.find_field(title="sum"),
                ds.find_field(title="ago"),
                ds.find_field(title="ago_igdim"),
                # "Bad" dimension:
                ds.find_field(title="day"),
            ],
            order_by=[
                ds.find_field(title="date_value"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)

        # [ago] should have all NULL values because of the "bad" dimension [day]
        for row_idx, row in enumerate(data_rows):
            assert row[2] is None, f"Expected a None value in row {row_idx}, but got {row[2]}"

        # [ago_igdim] should act the same way as a regular AGO under regular circumstances
        check_ago_data(data_rows=data_rows, date_idx=0, value_idx=1, ago_idx=3, day_offset=1)

    def test_at_date(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "Sales Sum": "SUM([sales])",
                "Sales Sum Fixed": "AT_DATE([Sales Sum], [order_date], #2014-02-02#)",
                "Sales Sum Nullable": 'AT_DATE([Sales Sum], [order_date], DATE_PARSE("2014-02-02"))',
                "Sales Sum Trunc": 'AT_DATE([Sales Sum], [order_date], DATETRUNC([order_date], "month"))',
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="category"),
                ds.find_field(title="order_date"),
                ds.find_field(title="Sales Sum"),
                ds.find_field(title="Sales Sum Fixed"),
                ds.find_field(title="Sales Sum Nullable"),
                ds.find_field(title="Sales Sum Trunc"),
            ],
            order_by=[
                ds.find_field(title="category"),
                ds.find_field(title="order_date"),
            ],
            filters=[
                ds.find_field(title="category").filter(op=WhereClauseOperation.EQ, values=["Office Supplies"]),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)
        check_at_date_data(
            data_rows=data_rows,
            date_idx=1,
            value_idx=2,
            ago_idx=3,
            ago_date_callable=lambda d: datetime.date(2014, 2, 2),
        )
        check_at_date_data(
            data_rows=data_rows,
            date_idx=1,
            value_idx=2,
            ago_idx=4,
            ago_date_callable=lambda d: datetime.date(2014, 2, 2),
        )
        check_at_date_data(
            data_rows=data_rows, date_idx=1, value_idx=2, ago_idx=5, ago_date_callable=lambda d: d.replace(day=1)
        )

    def test_at_date_with_measure_as_third_arg(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "Sales Sum": "SUM([sales])",
                "Profit Sum": "SUM([profit])",
                "Sum At Date": "AT_DATE([Sales Sum], [order_date], #2014-02-02# + [Profit Sum]*0)",
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="category"),
                ds.find_field(title="order_date"),
                ds.find_field(title="Sales Sum"),
                ds.find_field(title="Sum At Date"),
            ],
            order_by=[
                ds.find_field(title="category"),
                ds.find_field(title="order_date"),
            ],
            filters=[
                ds.find_field(title="category").filter(op=WhereClauseOperation.EQ, values=["Office Supplies"]),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)
        check_at_date_data(
            data_rows=data_rows,
            date_idx=1,
            value_idx=2,
            ago_idx=3,
            ago_date_callable=lambda d: datetime.date(2014, 2, 2),
        )

    def test_ago_with_bfb(self, control_api, data_api, saved_connection_id, db):
        day_offset = 3

        db_table = make_table(db)

        ds = create_basic_dataset(
            api_v1=control_api,
            connection_id=saved_connection_id,
            data_source_settings=data_source_settings_from_table(table=db_table),
            formulas={
                "sum": "SUM([int_value])",
                "date_duplicate": "[date_value]",
                "ago": f'AGO([sum], [date_value], "day", {day_offset})',
                "ago_bfb": f'AGO([sum], [date_value], "day", {day_offset} ' f"BEFORE FILTER BY [date_duplicate])",
                "ago_bfb_nested": (
                    f'AGO(AGO([sum], [date_value], "day", 1), [date_value], "day", {day_offset - 1} '
                    "BEFORE FILTER BY [date_duplicate])"
                ),
            },
        )

        min_date_s, _ = get_range_values(data_api.get_value_range(dataset=ds, field=ds.find_field(title="date_value")))
        min_date = read_date(min_date_s)
        gte_date_s = (min_date + datetime.timedelta(days=day_offset)).isoformat()

        def get_data_rows_with_filter(
            filters: Optional[list[WhereClause]] = None,
        ) -> list[list]:
            result_resp = data_api.get_result(
                dataset=ds,
                fields=[
                    ds.find_field(title="date_value"),
                    ds.find_field(title="sum"),
                    ds.find_field(title="ago"),
                    ds.find_field(title="ago_bfb"),
                    ds.find_field(title="ago_bfb_nested"),
                ],
                order_by=[
                    ds.find_field(title="date_value"),
                ],
                filters=filters,
                fail_ok=True,
            )
            assert result_resp.status_code == HTTPStatus.OK, result_resp.json
            return get_data_rows(result_resp)

        # Check default BFB (main dimension) in AGO without explicit BFB
        data_rows = get_data_rows_with_filter(
            filters=[
                # Apply filter to main dimension - it should be BFBed by default
                ds.find_field(title="date_value").filter(
                    # > min_date + {day_offset} days
                    op=WhereClauseOperation.GTE,
                    values=[gte_date_s],
                ),
            ]
        )
        check_ago_data(
            data_rows=data_rows,
            date_idx=0,
            value_idx=1,
            ago_idx=2,
            day_offset=day_offset,
            allow_missing_date_values=True,
        )
        # Now make sure there really are non-NULL values
        for row_idx in range(1, day_offset):  # Skip 0th row because because it has no AGO value
            assert data_rows[row_idx][2] is not None, f"Expected a non-None value in row {row_idx}"

        # Now check the explicit BFB (with filter for non-main dimension)
        data_rows = get_data_rows_with_filter(
            filters=[
                # Filter has to be applied to a dimension other than the one in AGO
                ds.find_field(title="date_duplicate").filter(
                    # > min_date + {day_offset} days
                    op=WhereClauseOperation.GTE,
                    values=[gte_date_s],
                ),
            ]
        )
        check_ago_data(
            data_rows=data_rows,
            date_idx=0,
            value_idx=1,
            ago_idx=2,
            day_offset=day_offset,
        )
        # Omit the first 2 rows because their values are not None
        check_ago_data(
            data_rows=data_rows,
            date_idx=0,
            value_idx=1,
            ago_idx=3,
            day_offset=day_offset,
            allow_missing_date_values=True,
        )
        check_ago_data(
            data_rows=data_rows,
            date_idx=0,
            value_idx=1,
            ago_idx=4,
            day_offset=day_offset,
            allow_missing_date_values=True,
        )
        # Now make sure there really are non-NULL values
        for row_idx in range(1, day_offset):  # Skip 0th row because because it has no AGO value
            assert data_rows[row_idx][3] is not None, f"Expected a non-None value in row {row_idx}"
            assert data_rows[row_idx][4] is not None, f"Expected a non-None value in row {row_idx}"

    def test_ago_with_corner_case_dimensions(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "Measure": "SUM([sales])",
                "Invalid Field": "[Whaaa?...]",
                "Invalid AGO": "AGO([Measure], [Invalid Field])",
            },
            exp_status=HTTPStatus.BAD_REQUEST,
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="order_date"),
                ds.find_field(title="Invalid Field"),
                ds.find_field(title="Invalid AGO"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.BAD_REQUEST

    def test_ago_with_different_measures(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "Measure 1": "SUM([sales])",
                "Measure 2": "COUNTD([sales])",
                "Ago Measure 1": "AGO([Measure 1], [order_date])",
                "Ago Measure 2": "AGO([Measure 2], [order_date])",
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="order_date"),
                ds.find_field(title="Measure 1"),
                ds.find_field(title="Measure 2"),
                ds.find_field(title="Ago Measure 1"),
                ds.find_field(title="Ago Measure 2"),
            ],
            order_by=[
                ds.find_field(title="order_date"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)
        check_ago_data(data_rows=data_rows, date_idx=0, value_idx=1, ago_idx=3, day_offset=1)
        check_ago_data(data_rows=data_rows, date_idx=0, value_idx=2, ago_idx=4, day_offset=1)
