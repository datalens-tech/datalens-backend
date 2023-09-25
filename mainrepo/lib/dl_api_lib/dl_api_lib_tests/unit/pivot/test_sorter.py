from dl_api_lib.pivot.hashable_packing import JsonHashableValuePacker
from dl_api_lib.pivot.pandas.transformer import PdPivotTransformer
from dl_api_lib.pivot.primitives import (
    PivotHeader,
    PivotHeaderInfo,
    PivotHeaderRoleSpec,
    PivotHeaderValue,
    PivotMeasureSorting,
    PivotMeasureSortingSettings,
)
from dl_api_lib.pivot.primitives import DataCell as DC
from dl_api_lib.pivot.primitives import DataCellVector as DV
from dl_api_lib.pivot.primitives import DataRow
from dl_api_lib.pivot.primitives import MeasureNameValue as MNV
from dl_api_lib.query.formalization.pivot_legend import (
    PivotDimensionRoleSpec,
    PivotLegend,
    PivotLegendItem,
    PivotMeasureRoleSpec,
)
from dl_constants.enums import (
    BIType,
    FieldType,
    OrderDirection,
    PivotHeaderRole,
    PivotItemType,
    PivotRole,
)
from dl_query_processing.legend.field_legend import (
    FieldObjSpec,
    Legend,
    LegendItem,
    MeasureNameObjSpec,
)
from dl_query_processing.merging.primitives import MergedQueryDataRow


def test_measure_sort_basic():
    fid_city, fid_ctgry, fid_sales = "123", "456", "789"
    liid_ctgry = 0
    liid_city = 1
    liid_mnames = 2
    liid_sales = 3
    legend_item_ids = (liid_city, liid_ctgry, liid_sales)

    data = [
        MergedQueryDataRow(data=("Detroit", "Furniture", 100), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=("Moscow", "Technology", 200), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=("San Francisco", "Furniture", 300), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=("San Francisco", "Office Supplies", 400), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=("Moscow", "Office Supplies", 500), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=("Detroit", "Office Supplies", 600), legend_item_ids=legend_item_ids),
    ]

    legend = Legend(
        items=[
            LegendItem(
                legend_item_id=liid_ctgry,
                obj=FieldObjSpec(id=fid_ctgry, title="Category"),
                field_type=FieldType.DIMENSION,
                data_type=BIType.string,
            ),
            LegendItem(
                legend_item_id=liid_city,
                obj=FieldObjSpec(id=fid_city, title="City"),
                field_type=FieldType.DIMENSION,
                data_type=BIType.string,
            ),
            LegendItem(
                legend_item_id=liid_mnames,
                obj=MeasureNameObjSpec(),
                field_type=FieldType.DIMENSION,
                data_type=BIType.string,
            ),
            LegendItem(
                legend_item_id=liid_sales,
                obj=FieldObjSpec(id=fid_sales, title="Sales"),
                field_type=FieldType.MEASURE,
                data_type=BIType.integer,
            ),
        ]
    )

    # Pivot item IDs
    piid_mnames = 10
    piid_city = 20
    piid_ctgry = 30
    piid_sales = 40
    pivot_legend = PivotLegend(
        items=[
            PivotLegendItem(
                pivot_item_id=piid_ctgry,
                legend_item_ids=[liid_ctgry],
                role_spec=PivotDimensionRoleSpec(role=PivotRole.pivot_column, direction=OrderDirection.desc),
                title="Category",
            ),
            PivotLegendItem(
                pivot_item_id=piid_city,
                legend_item_ids=[liid_city],
                role_spec=PivotDimensionRoleSpec(role=PivotRole.pivot_row, direction=OrderDirection.asc),
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
                            header_values=[PivotHeaderValue(value="Moscow")],
                            direction=OrderDirection.asc,
                            role_spec=PivotHeaderRoleSpec(role=PivotHeaderRole.data),
                        ),
                    ),
                ),
                title="Sales",
            ),
        ],
    )

    transformer = PdPivotTransformer(
        legend=legend,
        pivot_legend=pivot_legend,
        cell_packer=JsonHashableValuePacker(),
    )
    actual_pivot_table = transformer.pivot(data)
    actual_pivot_table.facade.sort()

    expected_pivot_columns = [
        PivotHeader(
            values=(DV(cells=(DC("Furniture", liid_ctgry, piid_ctgry),)),),
            info=PivotHeaderInfo(sorting_direction=OrderDirection.desc),
        ),
        PivotHeader(values=(DV(cells=(DC("Technology", liid_ctgry, piid_ctgry),)),)),
        PivotHeader(values=(DV(cells=(DC("Office Supplies", liid_ctgry, piid_ctgry),)),)),
    ]
    actual_pivot_columns = actual_pivot_table.get_columns()
    assert actual_pivot_columns == expected_pivot_columns

    expected_pivot_rows = [
        DataRow(
            header=PivotHeader(
                values=(
                    DV(cells=(DC("San Francisco", liid_city, piid_city),)),
                    DV(cells=(DC(MNV("Sales", piid_sales), liid_mnames, piid_mnames),)),
                )
            ),
            values=(
                DV(cells=(DC(300, liid_sales, piid_sales),)),
                None,
                DV(cells=(DC(400, liid_sales, piid_sales),)),
            ),
        ),
        DataRow(
            header=PivotHeader(
                values=(
                    DV(cells=(DC("Detroit", liid_city, piid_city),)),
                    DV(cells=(DC(MNV("Sales", piid_sales), liid_mnames, piid_mnames),)),
                )
            ),
            values=(
                DV(cells=(DC(100, liid_sales, piid_sales),)),
                None,
                DV(cells=(DC(600, liid_sales, piid_sales),)),
            ),
        ),
        DataRow(
            header=PivotHeader(
                values=(
                    DV(cells=(DC("Moscow", liid_city, piid_city),)),
                    DV(cells=(DC(MNV("Sales", piid_sales), liid_mnames, piid_mnames),)),
                ),
                info=PivotHeaderInfo(sorting_direction=OrderDirection.asc),
            ),
            values=(
                None,
                DV(cells=(DC(200, liid_sales, piid_sales),)),
                DV(cells=(DC(500, liid_sales, piid_sales),)),
            ),
        ),
    ]
    actual_pivot_rows = list(actual_pivot_table.get_rows())
    assert actual_pivot_rows == expected_pivot_rows


def test_measure_sort_no_rows_dimension():
    fid_ctgry, fid_sales = "123", "456"
    liid_ctgry = 0
    liid_mnames = 1
    liid_sales = 2
    legend_item_ids = (liid_ctgry, liid_sales)

    data = [
        MergedQueryDataRow(data=("Furniture", 200), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=("Office Supplies", 400), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=("Technology", 100), legend_item_ids=legend_item_ids),
    ]

    legend = Legend(
        items=[
            LegendItem(
                legend_item_id=liid_ctgry,
                obj=FieldObjSpec(id=fid_ctgry, title="Category"),
                field_type=FieldType.DIMENSION,
                data_type=BIType.string,
            ),
            LegendItem(
                legend_item_id=liid_mnames,
                obj=MeasureNameObjSpec(),
                field_type=FieldType.DIMENSION,
                data_type=BIType.string,
            ),
            LegendItem(
                legend_item_id=liid_sales,
                obj=FieldObjSpec(id=fid_sales, title="Sales"),
                field_type=FieldType.MEASURE,
                data_type=BIType.integer,
            ),
        ]
    )

    # Pivot item IDs
    piid_mnames = 10
    piid_ctgry = 20
    piid_sales = 30
    pivot_legend = PivotLegend(
        items=[
            PivotLegendItem(
                pivot_item_id=piid_ctgry,
                legend_item_ids=[liid_ctgry],
                role_spec=PivotDimensionRoleSpec(role=PivotRole.pivot_column),
                title="Category",
            ),
            PivotLegendItem(
                pivot_item_id=piid_mnames,
                item_type=PivotItemType.measure_name,
                legend_item_ids=[liid_mnames],
                role_spec=PivotDimensionRoleSpec(role=PivotRole.pivot_column),
                title="Measure Name",
            ),
            PivotLegendItem(
                pivot_item_id=piid_sales,
                legend_item_ids=[liid_sales],
                role_spec=PivotMeasureRoleSpec(
                    role=PivotRole.pivot_measure,
                    sorting=PivotMeasureSorting(
                        column=PivotMeasureSortingSettings(
                            header_values=[PivotHeaderValue(value="Office Supplies"), PivotHeaderValue(value="Sales")],
                            direction=OrderDirection.asc,
                            role_spec=PivotHeaderRoleSpec(role=PivotHeaderRole.data),
                        ),
                        row=None,
                    ),
                ),
                title="Sales",
            ),
        ],
    )

    transformer = PdPivotTransformer(
        legend=legend,
        pivot_legend=pivot_legend,
        cell_packer=JsonHashableValuePacker(),
    )
    actual_pivot_table = transformer.pivot(data)
    actual_pivot_table.facade.sort()

    # data should be left intact, as "sorting" was performed by column with only one value
    mnames_dv = DV(cells=(DC(MNV("Sales", piid_sales), liid_mnames, piid_mnames),))
    expected_pivot_columns = [
        PivotHeader(values=(DV(cells=(DC("Furniture", liid_ctgry, piid_ctgry),)), mnames_dv)),
        PivotHeader(
            values=(DV(cells=(DC("Office Supplies", liid_ctgry, piid_ctgry),)), mnames_dv),
            info=PivotHeaderInfo(sorting_direction=OrderDirection.asc),
        ),
        PivotHeader(values=(DV(cells=(DC("Technology", liid_ctgry, piid_ctgry),)), mnames_dv)),
    ]
    actual_pivot_columns = actual_pivot_table.get_columns()
    assert actual_pivot_columns == expected_pivot_columns

    expected_pivot_rows = [
        DataRow(
            header=PivotHeader(),
            values=(
                DV(cells=(DC(200, liid_sales, piid_sales),)),
                DV(cells=(DC(400, liid_sales, piid_sales),)),
                DV(cells=(DC(100, liid_sales, piid_sales),)),
            ),
        ),
    ]
    actual_pivot_rows = list(actual_pivot_table.get_rows())
    assert actual_pivot_rows == expected_pivot_rows


def test_measure_sort_with_multiple_measures():
    fid_city, fid_ctgry, fid_sales, fid_profit = ("123", "456", "789", "000")
    liid_ctgry = 0
    liid_city = 1
    liid_mnames = 2
    liid_sales = 3
    liid_profit = 4
    legend_item_ids = (liid_city, liid_ctgry, liid_sales, liid_profit)

    data = [
        MergedQueryDataRow(data=("Detroit", "Furniture", 100, 10), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=("Moscow", "Technology", 200, 20), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=("San Francisco", "Furniture", 300, 30), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=("San Francisco", "Office Supplies", 400, 40), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=("Moscow", "Office Supplies", 500, 50), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=("Detroit", "Office Supplies", 600, 60), legend_item_ids=legend_item_ids),
    ]

    legend = Legend(
        items=[
            LegendItem(
                legend_item_id=liid_ctgry,
                obj=FieldObjSpec(id=fid_ctgry, title="Category"),
                field_type=FieldType.DIMENSION,
                data_type=BIType.string,
            ),
            LegendItem(
                legend_item_id=liid_city,
                obj=FieldObjSpec(id=fid_city, title="City"),
                field_type=FieldType.DIMENSION,
                data_type=BIType.string,
            ),
            LegendItem(
                legend_item_id=liid_mnames,
                obj=MeasureNameObjSpec(),
                field_type=FieldType.DIMENSION,
                data_type=BIType.string,
            ),
            LegendItem(
                legend_item_id=liid_sales,
                obj=FieldObjSpec(id=fid_sales, title="Sales"),
                field_type=FieldType.MEASURE,
                data_type=BIType.integer,
            ),
            LegendItem(
                legend_item_id=liid_profit,
                obj=FieldObjSpec(id=fid_profit, title="Profit"),
                field_type=FieldType.MEASURE,
                data_type=BIType.integer,
            ),
        ]
    )

    # Pivot item IDs
    piid_mnames = 10
    piid_city = 20
    piid_ctgry = 30
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
                role_spec=PivotDimensionRoleSpec(role=PivotRole.pivot_row, direction=OrderDirection.desc),
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
                        column=None,
                        row=PivotMeasureSortingSettings(
                            header_values=[PivotHeaderValue(value="Moscow"), PivotHeaderValue(value="Sales")],
                            direction=OrderDirection.desc,
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

    transformer = PdPivotTransformer(
        legend=legend,
        pivot_legend=pivot_legend,
        cell_packer=JsonHashableValuePacker(),
    )
    actual_pivot_table = transformer.pivot(data)
    actual_pivot_table.facade.sort()

    expected_pivot_columns = [
        PivotHeader(values=(DV(cells=(DC("Office Supplies", liid_ctgry, piid_ctgry),)),)),
        PivotHeader(values=(DV(cells=(DC("Technology", liid_ctgry, piid_ctgry),)),)),
        PivotHeader(values=(DV(cells=(DC("Furniture", liid_ctgry, piid_ctgry),)),)),
    ]
    actual_pivot_columns = actual_pivot_table.get_columns()
    assert actual_pivot_columns == expected_pivot_columns

    expected_pivot_rows = [
        DataRow(
            header=PivotHeader(
                values=(
                    DV(cells=(DC("San Francisco", liid_city, piid_city),)),
                    DV(cells=(DC(MNV("Sales", piid_sales), liid_mnames, piid_mnames),)),
                )
            ),
            values=(
                DV(cells=(DC(400, liid_sales, piid_sales),)),
                None,
                DV(cells=(DC(300, liid_sales, piid_sales),)),
            ),
        ),
        DataRow(
            header=PivotHeader(
                values=(
                    DV(cells=(DC("San Francisco", liid_city, piid_city),)),
                    DV(cells=(DC(MNV("Profit", piid_profit), liid_mnames, piid_mnames),)),
                )
            ),
            values=(
                DV(cells=(DC(40, liid_profit, piid_profit),)),
                None,
                DV(cells=(DC(30, liid_profit, piid_profit),)),
            ),
        ),
        DataRow(
            header=PivotHeader(
                values=(
                    DV(cells=(DC("Moscow", liid_city, piid_city),)),
                    DV(cells=(DC(MNV("Sales", piid_sales), liid_mnames, piid_mnames),)),
                ),
                info=PivotHeaderInfo(sorting_direction=OrderDirection.desc),
            ),
            values=(
                DV(cells=(DC(500, liid_sales, piid_sales),)),
                DV(cells=(DC(200, liid_sales, piid_sales),)),
                None,
            ),
        ),
        DataRow(
            header=PivotHeader(
                values=(
                    DV(cells=(DC("Moscow", liid_city, piid_city),)),
                    DV(cells=(DC(MNV("Profit", piid_profit), liid_mnames, piid_mnames),)),
                )
            ),
            values=(
                DV(cells=(DC(50, liid_profit, piid_profit),)),
                DV(cells=(DC(20, liid_profit, piid_profit),)),
                None,
            ),
        ),
        DataRow(
            header=PivotHeader(
                values=(
                    DV(cells=(DC("Detroit", liid_city, piid_city),)),
                    DV(cells=(DC(MNV("Sales", piid_sales), liid_mnames, piid_mnames),)),
                )
            ),
            values=(
                DV(cells=(DC(600, liid_sales, piid_sales),)),
                None,
                DV(cells=(DC(100, liid_sales, piid_sales),)),
            ),
        ),
        DataRow(
            header=PivotHeader(
                values=(
                    DV(cells=(DC("Detroit", liid_city, piid_city),)),
                    DV(cells=(DC(MNV("Profit", piid_profit), liid_mnames, piid_mnames),)),
                )
            ),
            values=(
                DV(cells=(DC(60, liid_profit, piid_profit),)),
                None,
                DV(cells=(DC(10, liid_profit, piid_profit),)),
            ),
        ),
    ]
    actual_pivot_rows = list(actual_pivot_table.get_rows())
    assert actual_pivot_rows == expected_pivot_rows


def test_dimension_sort_with_multiple_measures():
    fid_city, fid_ctgry, fid_sales, fid_profit = ("123", "456", "789", "000")
    liid_ctgry = 0
    liid_city = 1
    liid_mnames = 2
    liid_sales = 3
    liid_profit = 4
    legend_item_ids = (liid_city, liid_ctgry, liid_sales, liid_profit)

    data = [
        MergedQueryDataRow(data=("Detroit", "Furniture", 100, 10), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=("Moscow", "Technology", 200, 20), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=("San Francisco", "Furniture", 300, 30), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=("San Francisco", "Office Supplies", 400, 40), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=("Moscow", "Office Supplies", 500, 50), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=("Detroit", "Office Supplies", 600, 60), legend_item_ids=legend_item_ids),
    ]

    legend = Legend(
        items=[
            LegendItem(
                legend_item_id=liid_ctgry,
                obj=FieldObjSpec(id=fid_ctgry, title="Category"),
                field_type=FieldType.DIMENSION,
                data_type=BIType.string,
            ),
            LegendItem(
                legend_item_id=liid_city,
                obj=FieldObjSpec(id=fid_city, title="City"),
                field_type=FieldType.DIMENSION,
                data_type=BIType.string,
            ),
            LegendItem(
                legend_item_id=liid_mnames,
                obj=MeasureNameObjSpec(),
                field_type=FieldType.DIMENSION,
                data_type=BIType.string,
            ),
            LegendItem(
                legend_item_id=liid_sales,
                obj=FieldObjSpec(id=fid_sales, title="Sales"),
                field_type=FieldType.MEASURE,
                data_type=BIType.integer,
            ),
            LegendItem(
                legend_item_id=liid_profit,
                obj=FieldObjSpec(id=fid_profit, title="Profit"),
                field_type=FieldType.MEASURE,
                data_type=BIType.integer,
            ),
        ]
    )

    # Pivot item IDs
    piid_mnames = 10
    piid_city = 20
    piid_ctgry = 30
    piid_sales = 40
    piid_profit = 50
    pivot_legend = PivotLegend(
        items=[
            PivotLegendItem(
                pivot_item_id=piid_ctgry,
                legend_item_ids=[liid_ctgry],
                role_spec=PivotDimensionRoleSpec(role=PivotRole.pivot_column, direction=OrderDirection.desc),
                title="Category",
            ),
            PivotLegendItem(
                pivot_item_id=piid_city,
                legend_item_ids=[liid_city],
                role_spec=PivotDimensionRoleSpec(role=PivotRole.pivot_row, direction=OrderDirection.asc),
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
                role_spec=PivotMeasureRoleSpec(role=PivotRole.pivot_measure),
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

    transformer = PdPivotTransformer(
        legend=legend,
        pivot_legend=pivot_legend,
        cell_packer=JsonHashableValuePacker(),
    )
    actual_pivot_table = transformer.pivot(data)
    actual_pivot_table.facade.sort()

    expected_pivot_columns = [
        # DESC
        PivotHeader(values=(DV(cells=(DC("Technology", liid_ctgry, piid_ctgry),)),)),
        PivotHeader(values=(DV(cells=(DC("Office Supplies", liid_ctgry, piid_ctgry),)),)),
        PivotHeader(values=(DV(cells=(DC("Furniture", liid_ctgry, piid_ctgry),)),)),
    ]
    actual_pivot_columns = actual_pivot_table.get_columns()
    assert actual_pivot_columns == expected_pivot_columns

    expected_pivot_rows = [
        # ASC by City;
        # Measure Name should have the same order as in the legend ('Sales', 'Profit')
        DataRow(
            header=PivotHeader(
                values=(
                    DV(cells=(DC("Detroit", liid_city, piid_city),)),
                    DV(cells=(DC(MNV("Sales", piid_sales), liid_mnames, piid_mnames),)),
                )
            ),
            values=(
                None,
                DV(cells=(DC(600, liid_sales, piid_sales),)),
                DV(cells=(DC(100, liid_sales, piid_sales),)),
            ),
        ),
        DataRow(
            header=PivotHeader(
                values=(
                    DV(cells=(DC("Detroit", liid_city, piid_city),)),
                    DV(cells=(DC(MNV("Profit", piid_profit), liid_mnames, piid_mnames),)),
                )
            ),
            values=(
                None,
                DV(cells=(DC(60, liid_profit, piid_profit),)),
                DV(cells=(DC(10, liid_profit, piid_profit),)),
            ),
        ),
        DataRow(
            header=PivotHeader(
                values=(
                    DV(cells=(DC("Moscow", liid_city, piid_city),)),
                    DV(cells=(DC(MNV("Sales", piid_sales), liid_mnames, piid_mnames),)),
                )
            ),
            values=(
                DV(cells=(DC(200, liid_sales, piid_sales),)),
                DV(cells=(DC(500, liid_sales, piid_sales),)),
                None,
            ),
        ),
        DataRow(
            header=PivotHeader(
                values=(
                    DV(cells=(DC("Moscow", liid_city, piid_city),)),
                    DV(cells=(DC(MNV("Profit", piid_profit), liid_mnames, piid_mnames),)),
                )
            ),
            values=(
                DV(cells=(DC(20, liid_profit, piid_profit),)),
                DV(cells=(DC(50, liid_profit, piid_profit),)),
                None,
            ),
        ),
        DataRow(
            header=PivotHeader(
                values=(
                    DV(cells=(DC("San Francisco", liid_city, piid_city),)),
                    DV(cells=(DC(MNV("Sales", piid_sales), liid_mnames, piid_mnames),)),
                )
            ),
            values=(
                None,
                DV(cells=(DC(400, liid_sales, piid_sales),)),
                DV(cells=(DC(300, liid_sales, piid_sales),)),
            ),
        ),
        DataRow(
            header=PivotHeader(
                values=(
                    DV(cells=(DC("San Francisco", liid_city, piid_city),)),
                    DV(cells=(DC(MNV("Profit", piid_profit), liid_mnames, piid_mnames),)),
                )
            ),
            values=(
                None,
                DV(cells=(DC(40, liid_profit, piid_profit),)),
                DV(cells=(DC(30, liid_profit, piid_profit),)),
            ),
        ),
    ]
    actual_pivot_rows = list(actual_pivot_table.get_rows())
    assert actual_pivot_rows == expected_pivot_rows


def test_dimension_sort_mixed_case_strings():
    fid_thing, fid_string, fid_measure = ("123", "456", "789")
    liid_thing, liid_string, liid_measure = (0, 1, 2)
    legend_item_ids = (liid_thing, liid_string, liid_measure)

    data = [
        MergedQueryDataRow(data=("This", "Abc", None), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=("This", "bCd", None), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=("This", "ab", None), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=("This", "CDE", None), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=("This", "cd", None), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=("This", "BC", None), legend_item_ids=legend_item_ids),
    ]

    # Legend item IDs
    legend = Legend(
        items=[
            LegendItem(
                legend_item_id=liid_thing,
                obj=FieldObjSpec(id=fid_thing, title="Thing"),
                field_type=FieldType.DIMENSION,
                data_type=BIType.string,
            ),
            LegendItem(
                legend_item_id=liid_string,
                obj=FieldObjSpec(id=fid_string, title="String"),
                field_type=FieldType.DIMENSION,
                data_type=BIType.string,
            ),
            LegendItem(
                legend_item_id=liid_measure,
                obj=FieldObjSpec(id=fid_measure, title="Measure"),
                field_type=FieldType.MEASURE,
                data_type=BIType.integer,
            ),
        ]
    )

    # Pivot item IDs
    piid_thing = 10
    piid_string = 20
    piid_measure = 30
    pivot_legend = PivotLegend(
        items=[
            PivotLegendItem(
                pivot_item_id=piid_thing,
                legend_item_ids=[liid_thing],
                role_spec=PivotDimensionRoleSpec(role=PivotRole.pivot_column),
                title="Thing",
            ),
            PivotLegendItem(
                pivot_item_id=piid_string,
                legend_item_ids=[liid_string],
                role_spec=PivotDimensionRoleSpec(role=PivotRole.pivot_row, direction=OrderDirection.asc),
                title="String",
            ),
            PivotLegendItem(
                pivot_item_id=piid_measure,
                legend_item_ids=[liid_measure],
                role_spec=PivotMeasureRoleSpec(role=PivotRole.pivot_measure),
                title="Measure",
            ),
        ],
    )

    transformer = PdPivotTransformer(
        legend=legend,
        pivot_legend=pivot_legend,
        cell_packer=JsonHashableValuePacker(),
    )
    actual_pivot_table = transformer.pivot(data)
    actual_pivot_table.facade.sort()

    assert len(actual_pivot_table.get_columns()) == 1

    expected_pivot_rows = [
        DataRow(
            header=PivotHeader(values=(DV(cells=(DC("ab", liid_string, piid_string),)),)),
            values=(DV(cells=(DC(None, liid_measure, piid_measure),)),),
        ),
        DataRow(
            header=PivotHeader(values=(DV(cells=(DC("Abc", liid_string, piid_string),)),)),
            values=(DV(cells=(DC(None, liid_measure, piid_measure),)),),
        ),
        DataRow(
            header=PivotHeader(values=(DV(cells=(DC("BC", liid_string, piid_string),)),)),
            values=(DV(cells=(DC(None, liid_measure, piid_measure),)),),
        ),
        DataRow(
            header=PivotHeader(values=(DV(cells=(DC("bCd", liid_string, piid_string),)),)),
            values=(DV(cells=(DC(None, liid_measure, piid_measure),)),),
        ),
        DataRow(
            header=PivotHeader(values=(DV(cells=(DC("cd", liid_string, piid_string),)),)),
            values=(DV(cells=(DC(None, liid_measure, piid_measure),)),),
        ),
        DataRow(
            header=PivotHeader(values=(DV(cells=(DC("CDE", liid_string, piid_string),)),)),
            values=(DV(cells=(DC(None, liid_measure, piid_measure),)),),
        ),
    ]
    actual_pivot_rows = list(actual_pivot_table.get_rows())
    assert actual_pivot_rows == expected_pivot_rows


def test_dimension_sort_stringified_numbers():
    fid_thing, fid_number, fid_measure = ("123", "456", "789")
    liid_thing, liid_number, liid_measure = (0, 1, 2)
    legend_item_ids = (liid_thing, liid_number, liid_measure)
    data = [
        MergedQueryDataRow(data=("This", "1", None), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=("This", "24", None), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=("This", "2", None), legend_item_ids=legend_item_ids),
        # None is a valid value in data from DB
        MergedQueryDataRow(data=("This", None, None), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=("This", "12", None), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=("This", "3", None), legend_item_ids=legend_item_ids),
    ]
    legend = Legend(
        items=[
            LegendItem(
                legend_item_id=liid_thing,
                obj=FieldObjSpec(id=fid_thing, title="Thing"),
                field_type=FieldType.DIMENSION,
                data_type=BIType.string,
            ),
            LegendItem(
                legend_item_id=liid_number,
                obj=FieldObjSpec(id=fid_number, title="Number"),
                field_type=FieldType.DIMENSION,
                data_type=BIType.integer,
            ),
            LegendItem(
                legend_item_id=liid_measure,
                obj=FieldObjSpec(id=fid_measure, title="Measure"),
                field_type=FieldType.MEASURE,
                data_type=BIType.integer,
            ),
        ]
    )

    # Pivot item IDs
    piid_thing = 10
    piid_number = 20
    piid_measure = 30
    pivot_legend = PivotLegend(
        items=[
            PivotLegendItem(
                pivot_item_id=piid_thing,
                legend_item_ids=[liid_thing],
                role_spec=PivotDimensionRoleSpec(role=PivotRole.pivot_column),
                title="Thing",
            ),
            PivotLegendItem(
                pivot_item_id=piid_number,
                legend_item_ids=[liid_number],
                role_spec=PivotDimensionRoleSpec(role=PivotRole.pivot_row, direction=OrderDirection.asc),
                title="Number",
            ),
            PivotLegendItem(
                pivot_item_id=piid_measure,
                legend_item_ids=[liid_measure],
                role_spec=PivotMeasureRoleSpec(role=PivotRole.pivot_measure),
                title="Measure",
            ),
        ],
    )

    transformer = PdPivotTransformer(
        legend=legend,
        pivot_legend=pivot_legend,
        cell_packer=JsonHashableValuePacker(),
    )
    actual_pivot_table = transformer.pivot(data)
    actual_pivot_table.facade.sort()

    assert len(actual_pivot_table.get_columns()) == 1

    expected_pivot_rows = [
        DataRow(
            header=PivotHeader(values=(DV(cells=(DC(None, liid_number, piid_number),)),)),
            values=(DV(cells=(DC(None, liid_measure, piid_measure),)),),
        ),
        DataRow(
            header=PivotHeader(values=(DV(cells=(DC("1", liid_number, piid_number),)),)),
            values=(DV(cells=(DC(None, liid_measure, piid_measure),)),),
        ),
        DataRow(
            header=PivotHeader(values=(DV(cells=(DC("2", liid_number, piid_number),)),)),
            values=(DV(cells=(DC(None, liid_measure, piid_measure),)),),
        ),
        DataRow(
            header=PivotHeader(values=(DV(cells=(DC("3", liid_number, piid_number),)),)),
            values=(DV(cells=(DC(None, liid_measure, piid_measure),)),),
        ),
        DataRow(
            header=PivotHeader(values=(DV(cells=(DC("12", liid_number, piid_number),)),)),
            values=(DV(cells=(DC(None, liid_measure, piid_measure),)),),
        ),
        DataRow(
            header=PivotHeader(values=(DV(cells=(DC("24", liid_number, piid_number),)),)),
            values=(DV(cells=(DC(None, liid_measure, piid_measure),)),),
        ),
    ]
    actual_pivot_rows = list(actual_pivot_table.get_rows())
    assert actual_pivot_rows == expected_pivot_rows
