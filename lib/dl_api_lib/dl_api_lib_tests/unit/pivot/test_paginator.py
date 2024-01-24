from dl_api_lib.pivot.pandas.transformer import PdPivotTransformer
from dl_api_lib.pivot.pivot_legend import (
    PivotDimensionRoleSpec,
    PivotLegend,
    PivotLegendItem,
    PivotMeasureRoleSpec,
)
from dl_api_lib.pivot.primitives import (
    PivotHeaderRoleSpec,
    PivotHeaderValue,
    PivotMeasureSorting,
    PivotMeasureSortingSettings,
)
from dl_api_lib.pivot.primitives import DataCell as DC
from dl_api_lib.pivot.primitives import DataCellVector as DV
from dl_api_lib.pivot.primitives import MeasureNameValue as MNV
from dl_constants.enums import (
    FieldType,
    OrderDirection,
    PivotHeaderRole,
    PivotItemType,
    PivotRole,
    UserDataType,
)
from dl_query_processing.legend.field_legend import (
    FieldObjSpec,
    Legend,
    LegendItem,
    MeasureNameObjSpec,
)
from dl_query_processing.merging.primitives import MergedQueryDataRow


def test_paginate():
    fid_city, fid_ctgry, fid_sales, fid_profit = ("123", "456", "789", "000")
    liid_ctgry, liid_city, liid_mnames, liid_sales, liid_profit = (0, 1, 2, 3, 4)
    legend_item_ids = (liid_city, liid_ctgry, liid_sales, liid_profit)

    data = [
        MergedQueryDataRow(data=("Detroit", "Furniture", 100, 10), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=("Moscow", "Technology", 200, 20), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=("San Francisco", "Furniture", 300, 30), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=("San Francisco", "Office Supplies", 400, 40), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=("Moscow", "Office Supplies", 500, 50), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=("Detroit", "Office Supplies", 600, 60), legend_item_ids=legend_item_ids),
    ]

    # Legend item IDs
    legend = Legend(
        items=[
            LegendItem(
                legend_item_id=liid_ctgry,
                obj=FieldObjSpec(id=fid_ctgry, title="Category"),
                field_type=FieldType.DIMENSION,
                data_type=UserDataType.string,
            ),
            LegendItem(
                legend_item_id=liid_city,
                obj=FieldObjSpec(id=fid_city, title="City"),
                field_type=FieldType.DIMENSION,
                data_type=UserDataType.string,
            ),
            LegendItem(
                legend_item_id=liid_mnames,
                obj=MeasureNameObjSpec(),
                field_type=FieldType.DIMENSION,
                data_type=UserDataType.string,
            ),
            LegendItem(
                legend_item_id=liid_sales,
                obj=FieldObjSpec(id=fid_sales, title="Sales"),
                field_type=FieldType.MEASURE,
                data_type=UserDataType.integer,
            ),
            LegendItem(
                legend_item_id=liid_profit,
                obj=FieldObjSpec(id=fid_profit, title="Profit"),
                field_type=FieldType.MEASURE,
                data_type=UserDataType.integer,
            ),
        ]
    )

    # Pivot item IDs
    piid_city = 10
    piid_ctgry = 20
    piid_mnames = 30
    piid_sales = 40
    piid_profit = 50
    pivot_legend = PivotLegend(
        items=[
            PivotLegendItem(
                pivot_item_id=piid_ctgry,
                legend_item_ids=[liid_ctgry],
                role_spec=PivotDimensionRoleSpec(role=PivotRole.pivot_column),
                title="Category",
            ),
            PivotLegendItem(
                pivot_item_id=piid_city,
                legend_item_ids=[liid_city],
                role_spec=PivotDimensionRoleSpec(role=PivotRole.pivot_row),
                title="City",
            ),
            PivotLegendItem(
                pivot_item_id=piid_mnames,
                item_type=PivotItemType.measure_name,
                legend_item_ids=[liid_mnames],
                role_spec=PivotDimensionRoleSpec(role=PivotRole.pivot_row),
                title="Measure Name",
            ),
            PivotLegendItem(
                pivot_item_id=piid_sales,
                legend_item_ids=[liid_sales],
                role_spec=PivotMeasureRoleSpec(
                    role=PivotRole.pivot_measure,
                    sorting=PivotMeasureSorting(
                        column=PivotMeasureSortingSettings(
                            header_values=[PivotHeaderValue(value="Furniture")],
                            direction=OrderDirection.desc,
                            role_spec=PivotHeaderRoleSpec(role=PivotHeaderRole.data),
                        ),
                        row=PivotMeasureSortingSettings(
                            header_values=[PivotHeaderValue(value="Moscow"), PivotHeaderValue(value="Sales")],
                            direction=OrderDirection.asc,
                            role_spec=PivotHeaderRoleSpec(role=PivotHeaderRole.data),
                        ),
                    ),
                ),
                title="Sales",
            ),
            PivotLegendItem(
                pivot_item_id=piid_profit,
                legend_item_ids=[liid_profit],
                role_spec=PivotMeasureRoleSpec(role=PivotRole.pivot_measure),
                title="Profit",
            ),
        ],
    )

    transformer = PdPivotTransformer(legend=legend, pivot_legend=pivot_legend)
    pivot_table = transformer.pivot(data)
    pivot_table.facade.sort()

    initial_columns = list(pivot_table.get_columns())
    initial_rows = list(pivot_table.get_rows())

    offset_rows = 1
    limit_rows = 3
    pivot_table.facade.paginate(offset_rows=offset_rows, limit_rows=limit_rows)

    paginated_columns = list(pivot_table.get_columns())
    paginated_rows = list(pivot_table.get_rows())

    assert paginated_columns == initial_columns
    assert paginated_rows == initial_rows[offset_rows : offset_rows + limit_rows]

    # check that paginator preserves headers' info
    sorting_headers = [
        (DV(cells=(DC("Furniture", liid_ctgry, piid_ctgry),)),),
        (
            DV(cells=(DC("Moscow", liid_city, piid_city),)),
            DV(cells=(DC(MNV("Sales", piid_sales), liid_mnames, piid_mnames),)),
        ),
    ]
    # (Moscow, Sales) header is paginated, so the check is a bit hacky
    for header, direction in zip(sorting_headers, [OrderDirection.desc, OrderDirection.asc]):
        assert pivot_table.facade._pivot_dframe.headers_info[header].sorting_direction == direction
