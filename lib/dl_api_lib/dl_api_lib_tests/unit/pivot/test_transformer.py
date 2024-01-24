import pytest

from dl_api_lib.pivot.pandas.transformer import PdPivotTransformer
from dl_api_lib.pivot.pivot_legend import (
    PivotAnnotationRoleSpec,
    PivotDimensionRoleSpec,
    PivotLegend,
    PivotLegendItem,
    PivotMeasureRoleSpec,
)
from dl_api_lib.pivot.primitives import DataCell as DC
from dl_api_lib.pivot.primitives import DataCellVector as DV
from dl_api_lib.pivot.primitives import DataRow
from dl_api_lib.pivot.primitives import MeasureNameValue as MNV
from dl_api_lib.pivot.primitives import PivotHeader
from dl_constants.enums import (
    FieldType,
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


def test_pivot_single_measure():
    fid_city, fid_ctgry, fid_sales = ("123", "456", "789")
    liid_city, liid_ctgry, liid_sales = (0, 1, 2)
    legend_item_ids = (liid_city, liid_ctgry, liid_sales)

    data = [
        MergedQueryDataRow(data=("Detroit", "Furniture", 100), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=("San Francisco", "Furniture", 200), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=("Moscow", "Office Supplies", 300), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=("Detroit", "Office Supplies", 400), legend_item_ids=legend_item_ids),
    ]

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
                legend_item_id=liid_sales,
                obj=FieldObjSpec(id=fid_sales, title="Sales"),
                field_type=FieldType.MEASURE,
                data_type=UserDataType.integer,
            ),
        ]
    )

    piid_city, piid_ctgry, piid_sales = (10, 20, 30)
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
                pivot_item_id=piid_sales,
                legend_item_ids=[liid_sales],
                role_spec=PivotMeasureRoleSpec(role=PivotRole.pivot_measure),
                title="Sales",
            ),
        ],
    )

    transformer = PdPivotTransformer(legend=legend, pivot_legend=pivot_legend)
    actual_pivot_table = transformer.pivot(data)

    expected_pivot_columns = [
        PivotHeader(values=(DV(cells=(DC("Furniture", liid_ctgry, piid_ctgry),)),)),
        PivotHeader(values=(DV(cells=(DC("Office Supplies", liid_ctgry, piid_ctgry),)),)),
    ]
    actual_pivot_columns = actual_pivot_table.get_columns()
    assert actual_pivot_columns == expected_pivot_columns

    expected_pivot_rows = [
        DataRow(
            header=PivotHeader(values=(DV(cells=(DC("Detroit", liid_city, piid_city),)),)),
            values=(DV(cells=(DC(100, liid_sales, piid_sales),)), DV(cells=(DC(400, liid_sales, piid_sales),))),
        ),
        DataRow(
            header=PivotHeader(values=(DV(cells=(DC("Moscow", liid_city, piid_city),)),)),
            values=(None, DV(cells=(DC(300, liid_sales, piid_sales),))),
        ),
        DataRow(
            header=PivotHeader(values=(DV(cells=(DC("San Francisco", liid_city, piid_city),)),)),
            values=(DV(cells=(DC(200, liid_sales, piid_sales),)), None),
        ),
    ]
    actual_pivot_rows = list(actual_pivot_table.get_rows())
    assert actual_pivot_rows == expected_pivot_rows


def test_pivot_no_measures():
    fid_city, fid_ctgry = ("123", "456")
    liid_city, liid_ctgry = (0, 1)
    legend_item_ids = (liid_city, liid_ctgry)

    data = [
        MergedQueryDataRow(data=("Detroit", "Furniture"), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=("San Francisco", "Furniture"), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=("Moscow", "Office Supplies"), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=("Detroit", "Office Supplies"), legend_item_ids=legend_item_ids),
    ]

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
        ]
    )

    piid_city, piid_ctgry = (10, 20)
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
        ],
    )

    transformer = PdPivotTransformer(legend=legend, pivot_legend=pivot_legend)
    actual_pivot_table = transformer.pivot(data)

    expected_pivot_columns = [
        PivotHeader(values=(DV(cells=(DC("Furniture", liid_ctgry, piid_ctgry),)),)),
        PivotHeader(values=(DV(cells=(DC("Office Supplies", liid_ctgry, piid_ctgry),)),)),
    ]
    actual_pivot_columns = actual_pivot_table.get_columns()
    assert actual_pivot_columns == expected_pivot_columns

    expected_pivot_rows = [
        DataRow(
            header=PivotHeader(values=(DV(cells=(DC("Detroit", liid_city, piid_city),)),)),
            values=(None, None),
        ),
        DataRow(
            header=PivotHeader(values=(DV(cells=(DC("Moscow", liid_city, piid_city),)),)),
            values=(None, None),
        ),
        DataRow(
            header=PivotHeader(values=(DV(cells=(DC("San Francisco", liid_city, piid_city),)),)),
            values=(None, None),
        ),
    ]
    actual_pivot_rows = list(actual_pivot_table.get_rows())
    assert actual_pivot_rows == expected_pivot_rows


def test_pivot_multiple_measures():
    fid_city, fid_ctgry, fid_sales, fid_profit = ("123", "456", "789", "000")
    liid_city, liid_ctgry, liid_mnames, liid_sales, liid_profit = (0, 1, 2, 3, 4)
    legend_item_ids = (liid_city, liid_ctgry, liid_sales, liid_profit)

    data = [
        MergedQueryDataRow(data=("Detroit", "Furniture", 100, 10), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=("San Francisco", "Furniture", 200, 20), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=("Moscow", "Office Supplies", 300, 30), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=("Detroit", "Office Supplies", 400, 40), legend_item_ids=legend_item_ids),
    ]

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

    transformer = PdPivotTransformer(legend=legend, pivot_legend=pivot_legend)
    actual_pivot_table = transformer.pivot(data)

    expected_pivot_columns = [
        PivotHeader(values=(DV(cells=(DC("Furniture", liid_ctgry, piid_ctgry),)),)),
        PivotHeader(values=(DV(cells=(DC("Office Supplies", liid_ctgry, piid_ctgry),)),)),
    ]
    actual_pivot_columns = actual_pivot_table.get_columns()
    assert actual_pivot_columns == expected_pivot_columns

    expected_pivot_rows = [
        DataRow(
            header=PivotHeader(
                values=(
                    DV(cells=(DC("Detroit", liid_city, piid_city),)),
                    DV(cells=(DC(MNV("Profit", piid_profit), liid_mnames, piid_mnames),)),
                )
            ),
            values=(
                DV(cells=(DC(10, liid_profit, piid_profit),)),
                DV(cells=(DC(40, liid_profit, piid_profit),)),
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
                DV(cells=(DC(400, liid_sales, piid_sales),)),
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
                None,
                DV(cells=(DC(30, liid_profit, piid_profit),)),
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
                DV(cells=(DC(20, liid_profit, piid_profit),)),
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
                DV(cells=(DC(200, liid_sales, piid_sales),)),
                None,
            ),
        ),
    ]
    actual_pivot_rows = list(actual_pivot_table.get_rows())
    assert actual_pivot_rows == expected_pivot_rows


def test_pivot_multiple_measures_and_annotations():
    fid_city, fid_ctgry, fid_sales, fid_profit, fid_customers, fid_orders = (
        "123",
        "456",
        "789",
        "000",
        "222",
        "333",
    )
    # Legend item IDs:
    liid_mnames = 0
    liid_city = 1
    liid_ctgry = 2
    liid_sales = 3
    liid_profit = 4
    liid_customers = 5
    liid_orders = 6
    legend_item_ids = (liid_city, liid_ctgry, liid_sales, liid_profit, liid_customers, liid_orders)
    data = [
        MergedQueryDataRow(data=("Detroit", "Furniture", 100, 10, 3, 4), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=("San Francisco", "Furniture", 200, 20, 5, 7), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=("Moscow", "Office Supplies", 300, 30, 4, 4), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=("Detroit", "Office Supplies", 400, 40, 6, 10), legend_item_ids=legend_item_ids),
    ]
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
            LegendItem(
                legend_item_id=liid_customers,
                obj=FieldObjSpec(id=fid_customers, title="Customers"),
                field_type=FieldType.MEASURE,
                data_type=UserDataType.integer,
            ),
            LegendItem(
                legend_item_id=liid_orders,
                obj=FieldObjSpec(id=fid_orders, title="Orders"),
                field_type=FieldType.MEASURE,
                data_type=UserDataType.integer,
            ),
        ]
    )

    # Pivot item IDs
    piid_mnames = 10
    piid_city = 20
    piid_ctgry = 30
    piid_sales = 40
    piid_profit = 50
    piid_customers = 60
    piid_orders = 70
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
                role_spec=PivotMeasureRoleSpec(role=PivotRole.pivot_measure),
                title="Sales",
            ),
            PivotLegendItem(
                pivot_item_id=piid_profit,
                legend_item_ids=[liid_profit],
                role_spec=PivotMeasureRoleSpec(role=PivotRole.pivot_measure),
                title="Profit",
            ),
            PivotLegendItem(
                pivot_item_id=piid_customers,
                legend_item_ids=[liid_customers],
                role_spec=PivotAnnotationRoleSpec(role=PivotRole.pivot_annotation, annotation_type="color"),
                title="Customers",
            ),
            PivotLegendItem(
                pivot_item_id=piid_orders,
                legend_item_ids=[liid_orders],
                role_spec=PivotAnnotationRoleSpec(
                    role=PivotRole.pivot_annotation,
                    annotation_type="color",
                    target_legend_item_ids=[liid_profit],
                ),
                title="Orders",
            ),
        ],
    )

    transformer = PdPivotTransformer(legend=legend, pivot_legend=pivot_legend)
    actual_pivot_table = transformer.pivot(data)

    expected_pivot_columns = [
        PivotHeader(values=(DV(cells=(DC("Furniture", liid_ctgry, piid_ctgry),)),)),
        PivotHeader(values=(DV(cells=(DC("Office Supplies", liid_ctgry, piid_ctgry),)),)),
    ]
    actual_pivot_columns = actual_pivot_table.get_columns()
    assert actual_pivot_columns == expected_pivot_columns

    expected_pivot_rows = [
        DataRow(
            header=PivotHeader(
                values=(
                    DV(cells=(DC("Detroit", liid_city, piid_city),)),
                    DV(cells=(DC(MNV("Profit", piid_profit), liid_mnames, piid_mnames),)),
                )
            ),
            values=(
                DV(
                    cells=(
                        DC(10, liid_profit, piid_profit),
                        DC(3, liid_customers, piid_customers),
                        DC(4, liid_orders, piid_orders),
                    )
                ),
                DV(
                    cells=(
                        DC(40, liid_profit, piid_profit),
                        DC(6, liid_customers, piid_customers),
                        DC(10, liid_orders, piid_orders),
                    )
                ),
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
                DV(
                    cells=(
                        DC(100, liid_sales, piid_sales),
                        DC(3, liid_customers, piid_customers),
                    )
                ),
                DV(
                    cells=(
                        DC(400, liid_sales, piid_sales),
                        DC(6, liid_customers, piid_customers),
                    )
                ),
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
                None,
                DV(
                    cells=(
                        DC(30, liid_profit, piid_profit),
                        DC(4, liid_customers, piid_customers),
                        DC(4, liid_orders, piid_orders),
                    )
                ),
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
                None,
                DV(
                    cells=(
                        DC(300, liid_sales, piid_sales),
                        DC(4, liid_customers, piid_customers),
                    )
                ),
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
                DV(
                    cells=(
                        DC(20, liid_profit, piid_profit),
                        DC(5, liid_customers, piid_customers),
                        DC(7, liid_orders, piid_orders),
                    )
                ),
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
                DV(
                    cells=(
                        DC(200, liid_sales, piid_sales),
                        DC(5, liid_customers, piid_customers),
                    )
                ),
                None,
            ),
        ),
    ]
    actual_pivot_rows = list(actual_pivot_table.get_rows())
    assert actual_pivot_rows == expected_pivot_rows


def check_diagonal_pivot_table(row_cnt: int):
    fid_city, fid_ctgry, fid_sales = ("123", "456", "789")
    liid_ctgry, liid_city, liid_sales = (0, 1, 2)
    legend_item_ids = (liid_city, liid_ctgry, liid_sales)

    def city_gen(num):
        return f"City {num:020}"

    def cat_gen(num):
        return f"Cat no. {num:020}"

    data = [
        MergedQueryDataRow(data=(city_gen(num), cat_gen(num), num), legend_item_ids=legend_item_ids)
        for num in range(row_cnt)
    ]
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
                legend_item_id=liid_sales,
                obj=FieldObjSpec(id=fid_sales, title="Sales"),
                field_type=FieldType.MEASURE,
                data_type=UserDataType.integer,
            ),
        ]
    )

    piid_city, piid_ctgry, piid_sales = (10, 20, 30)
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
                pivot_item_id=piid_sales,
                legend_item_ids=[liid_sales],
                role_spec=PivotMeasureRoleSpec(role=PivotRole.pivot_measure),
                title="Sales",
            ),
        ],
    )

    transformer = PdPivotTransformer(legend=legend, pivot_legend=pivot_legend)
    actual_pivot_table = transformer.pivot(data)

    expected_pivot_columns = [
        PivotHeader(values=(DV(cells=(DC(cat_gen(col_num), liid_ctgry, piid_ctgry),)),)) for col_num in range(row_cnt)
    ]
    actual_pivot_columns = actual_pivot_table.get_columns()
    assert actual_pivot_columns == expected_pivot_columns

    expected_pivot_rows = [
        DataRow(
            header=PivotHeader(values=(DV(cells=(DC(city_gen(row_num), liid_city, piid_city),)),)),
            values=(
                *[None for _ in range(row_num)],
                DV(cells=(DC(row_num, liid_sales, piid_sales),)),
                *[None for _ in range(row_num + 1, row_cnt)],
            ),
        )
        for row_num in range(row_cnt)
    ]
    actual_pivot_rows = list(actual_pivot_table.get_rows())
    assert actual_pivot_rows == expected_pivot_rows


def test_diagonal_pivot():
    check_diagonal_pivot_table(100)


@pytest.mark.skip  # too much memory in CI
def test_pivot_overflow():
    with pytest.raises(MemoryError):
        check_diagonal_pivot_table(50000)
