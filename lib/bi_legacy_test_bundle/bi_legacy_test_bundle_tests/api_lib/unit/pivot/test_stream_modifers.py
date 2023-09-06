from bi_constants.enums import PivotRole, PivotItemType, FieldRole, FieldType, BIType

from bi_query_processing.legend.field_legend import Legend, LegendItem, RowRoleSpec, FieldObjSpec
from bi_api_lib.query.formalization.pivot_legend import (
    PivotLegend, PivotLegendItem, PivotMeasureRoleSpec, PivotDimensionRoleSpec, PivotAnnotationRoleSpec,
)
from bi_api_lib.pivot.primitives import DataCell as DC, DataCellVector as DV, MeasureNameValue as MNV
from bi_api_lib.pivot.stream_modifiers import DataCellConverter, MeasureDataTransposer, TransposedDataRow
from bi_api_lib.pivot.hashable_packing import JsonHashableValuePacker, JsonWrapper as JW
from bi_query_processing.merging.primitives import MergedQueryDataRow


def test_data_cell_converter():
    liid_ctgry, liid_city, liid_sales, liid_profit = (0, 1, 2, 3)
    legend_item_ids = (liid_city, liid_ctgry, liid_sales, liid_profit)
    data = [
        MergedQueryDataRow(data=('Detroit', 'Furniture', 100, 10), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=('Portland', 'Furniture', 200, 20), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=('Moscow', 'Plants', 300, 30), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=('Detroit', 'Plants', 400, 40), legend_item_ids=legend_item_ids),
    ]

    legend = Legend(
        items=[
            LegendItem(
                legend_item_id=liid_ctgry,
                role_spec=RowRoleSpec(role=FieldRole.row),
                obj=FieldObjSpec(id='1', title='Category'),
                field_type=FieldType.DIMENSION,
                data_type=BIType.string,
            ),
            LegendItem(
                legend_item_id=liid_city,
                role_spec=RowRoleSpec(role=FieldRole.row),
                obj=FieldObjSpec(id='1', title='City'),
                field_type=FieldType.DIMENSION,
                data_type=BIType.string,
            ),
            LegendItem(
                legend_item_id=liid_sales,
                role_spec=RowRoleSpec(role=FieldRole.row),
                obj=FieldObjSpec(id='1', title='Sales'),
                field_type=FieldType.MEASURE,
                data_type=BIType.float,
            ),
            LegendItem(
                legend_item_id=liid_profit,
                role_spec=RowRoleSpec(role=FieldRole.row),
                obj=FieldObjSpec(id='1', title='Profit'),
                field_type=FieldType.MEASURE,
                data_type=BIType.float,
            ),
        ],
    )

    # Pivot item IDs
    piid_ctgry = 10
    piid_city = 20
    piid_sales = 30
    piid_profit = 40
    pivot_legend = PivotLegend(
        items=[
            PivotLegendItem(
                pivot_item_id=piid_ctgry, legend_item_ids=[liid_ctgry],
                role_spec=PivotDimensionRoleSpec(role=PivotRole.pivot_row),
                title='Category',
            ),
            PivotLegendItem(
                pivot_item_id=piid_city, legend_item_ids=[liid_city],
                role_spec=PivotDimensionRoleSpec(role=PivotRole.pivot_column),
                title='City',
            ),
            PivotLegendItem(
                pivot_item_id=piid_sales, legend_item_ids=[liid_sales],
                role_spec=PivotMeasureRoleSpec(role=PivotRole.pivot_measure),
                title='Sales',
            ),
            PivotLegendItem(
                pivot_item_id=piid_profit, legend_item_ids=[liid_profit],
                role_spec=PivotMeasureRoleSpec(role=PivotRole.pivot_measure),
                title='Profit',
            ),
        ],
    )

    dcell_stream = DataCellConverter(
        rows=data, cell_packer=JsonHashableValuePacker(),
        legend=legend, pivot_legend=pivot_legend,
    )
    converted_data = list(dcell_stream)
    expected_data = [
        [
            DC('Detroit', liid_city, piid_city),
            DC('Furniture', liid_ctgry, piid_ctgry),
            DC(100, liid_sales, piid_sales),
            DC(10, liid_profit, piid_profit),
        ],
        [
            DC('Portland', liid_city, piid_city),
            DC('Furniture', liid_ctgry, piid_ctgry),
            DC(200, liid_sales, piid_sales),
            DC(20, liid_profit, piid_profit),
        ],
        [
            DC('Moscow', liid_city, piid_city),
            DC('Plants', liid_ctgry, piid_ctgry),
            DC(300, liid_sales, piid_sales),
            DC(30, liid_profit, piid_profit),
        ],
        [
            DC('Detroit', liid_city, piid_city),
            DC('Plants', liid_ctgry, piid_ctgry),
            DC(400, liid_sales, piid_sales),
            DC(40, liid_profit, piid_profit),
        ],
    ]
    assert converted_data == expected_data


def test_measure_data_transposer():
    # Legend item IDs
    liid_mnames = 0
    liid_city = 1
    liid_ctgry = 2
    liid_sales = 3
    liid_profit = 4

    # Pivot item IDs
    piid_mnames = 10
    piid_city = 20
    piid_ctgry = 30
    piid_sales = 40
    piid_profit = 50

    pivot_legend = PivotLegend(
        items=[
            PivotLegendItem(
                pivot_item_id=piid_mnames, item_type=PivotItemType.measure_name, legend_item_ids=[liid_mnames],
                role_spec=PivotDimensionRoleSpec(role=PivotRole.pivot_row),
                title='Measure Name',
            ),
            PivotLegendItem(
                pivot_item_id=piid_city, legend_item_ids=[liid_city],
                role_spec=PivotDimensionRoleSpec(role=PivotRole.pivot_row),
                title='City',
            ),
            PivotLegendItem(
                pivot_item_id=piid_ctgry, legend_item_ids=[liid_ctgry],
                role_spec=PivotDimensionRoleSpec(role=PivotRole.pivot_column),
                title='Category',
            ),
            PivotLegendItem(
                pivot_item_id=piid_sales, legend_item_ids=[liid_sales],
                role_spec=PivotMeasureRoleSpec(role=PivotRole.pivot_measure),
                title='Sales',
            ),
            PivotLegendItem(
                pivot_item_id=piid_profit, legend_item_ids=[liid_profit],
                role_spec=PivotMeasureRoleSpec(role=PivotRole.pivot_measure),
                title='Profit',
            ),
        ],
    )

    dcell_stream = [
        [
            DC('Detroit', liid_city, piid_city),
            DC('Furniture', liid_ctgry, piid_ctgry),
            DC(100, liid_sales, piid_sales),
            DC(10, liid_profit, piid_profit),
        ],
        [
            DC('Portland', liid_city, piid_city),
            DC('Furniture', liid_ctgry, piid_ctgry),
            DC(200, liid_sales, piid_sales),
            DC(20, liid_profit, piid_profit),
        ],
        [
            DC('Moscow', liid_city, piid_city),
            DC('Plants', liid_ctgry, piid_ctgry),
            DC(300, liid_sales, piid_sales),
            DC(30, liid_profit, piid_profit),
        ],
        [
            DC('Detroit', liid_city, piid_city),
            DC('Plants', liid_ctgry, piid_ctgry),
            DC(400, liid_sales, piid_sales),
            DC(40, liid_profit, piid_profit),
        ],
    ]

    transposed_stream = MeasureDataTransposer(
        dcell_stream=dcell_stream, pivot_legend=pivot_legend,
        cell_packer=JsonHashableValuePacker(),
    )
    transposed_data = list(transposed_stream)
    expected_data = [
        TransposedDataRow(
            dimensions=(
                DV(cells=(DC('Detroit', liid_city, piid_city),)),
                DV(cells=(DC('Furniture', liid_ctgry, piid_ctgry),)),
                DV(cells=(DC(MNV('Sales', piid_sales), liid_mnames, piid_mnames),)),
            ),
            value=DV(cells=(DC(100, liid_sales, piid_sales),)),
        ),
        TransposedDataRow(
            dimensions=(
                DV(cells=(DC('Detroit', liid_city, piid_city),)),
                DV(cells=(DC('Furniture', liid_ctgry, piid_ctgry),)),
                DV(cells=(DC(MNV('Profit', piid_profit), liid_mnames, piid_mnames),)),
            ),
            value=DV(cells=(DC(10, liid_profit, piid_profit),)),
        ),
        TransposedDataRow(
            dimensions=(
                DV(cells=(DC('Portland', liid_city, piid_city),)),
                DV(cells=(DC('Furniture', liid_ctgry, piid_ctgry),)),
                DV(cells=(DC(MNV('Sales', piid_sales), liid_mnames, piid_mnames),)),
            ),
            value=DV(cells=(DC(200, liid_sales, piid_sales),)),
        ),
        TransposedDataRow(
            dimensions=(
                DV(cells=(DC('Portland', liid_city, piid_city),)),
                DV(cells=(DC('Furniture', liid_ctgry, piid_ctgry),)),
                DV(cells=(DC(MNV('Profit', piid_profit), liid_mnames, piid_mnames),)),
            ),
            value=DV(cells=(DC(20, liid_profit, piid_profit),)),
        ),
        TransposedDataRow(
            dimensions=(
                DV(cells=(DC('Moscow', liid_city, piid_city),)),
                DV(cells=(DC('Plants', liid_ctgry, piid_ctgry),)),
                DV(cells=(DC(MNV('Sales', piid_sales), liid_mnames, piid_mnames),)),
            ),
            value=DV(cells=(DC(300, liid_sales, piid_sales),)),
        ),
        TransposedDataRow(
            dimensions=(
                DV(cells=(DC('Moscow', liid_city, piid_city),)),
                DV(cells=(DC('Plants', liid_ctgry, piid_ctgry),)),
                DV(cells=(DC(MNV('Profit', piid_profit), liid_mnames, piid_mnames),)),
            ),
            value=DV(cells=(DC(30, liid_profit, piid_profit),)),
        ),
        TransposedDataRow(
            dimensions=(
                DV(cells=(DC('Detroit', liid_city, piid_city),)),
                DV(cells=(DC('Plants', liid_ctgry, piid_ctgry),)),
                DV(cells=(DC(MNV('Sales', piid_sales), liid_mnames, piid_mnames),)),
            ),
            value=DV(cells=(DC(400, liid_sales, piid_sales),)),
        ),
        TransposedDataRow(
            dimensions=(
                DV(cells=(DC('Detroit', liid_city, piid_city),)),
                DV(cells=(DC('Plants', liid_ctgry, piid_ctgry),)),
                DV(cells=(DC(MNV('Profit', piid_profit), liid_mnames, piid_mnames),)),
            ),
            value=DV(cells=(DC(40, liid_profit, piid_profit),)),
        ),
    ]
    assert transposed_data == expected_data


def test_measure_data_transposer_with_annotations():
    # Legend item IDs:
    liid_mnames = 0
    liid_city = 1
    liid_ctgry = 2
    liid_sales = 3
    liid_profit = 4
    liid_customers = 5
    liid_orders = 6

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
                pivot_item_id=piid_mnames, item_type=PivotItemType.measure_name, legend_item_ids=[liid_mnames],
                role_spec=PivotDimensionRoleSpec(role=PivotRole.pivot_row),
                title='Measure Name',
            ),
            PivotLegendItem(
                pivot_item_id=piid_city, legend_item_ids=[liid_city],
                role_spec=PivotDimensionRoleSpec(role=PivotRole.pivot_row),
                title='City',
            ),
            PivotLegendItem(
                pivot_item_id=piid_ctgry, legend_item_ids=[liid_ctgry],
                role_spec=PivotDimensionRoleSpec(role=PivotRole.pivot_row),
                title='Category',
            ),
            PivotLegendItem(
                pivot_item_id=piid_sales, legend_item_ids=[liid_sales],
                role_spec=PivotMeasureRoleSpec(role=PivotRole.pivot_measure),
                title='Sales',
            ),
            PivotLegendItem(
                pivot_item_id=piid_profit, legend_item_ids=[liid_profit],
                role_spec=PivotMeasureRoleSpec(role=PivotRole.pivot_measure),
                title='Profit',
            ),
            PivotLegendItem(
                pivot_item_id=piid_customers, legend_item_ids=[liid_customers],
                role_spec=PivotAnnotationRoleSpec(role=PivotRole.pivot_annotation, annotation_type='color'),
                title='Customers',
            ),
            PivotLegendItem(
                pivot_item_id=piid_orders, legend_item_ids=[liid_orders],
                role_spec=PivotAnnotationRoleSpec(
                    role=PivotRole.pivot_annotation, annotation_type='color',
                    target_legend_item_ids=[liid_profit],
                ),
                title='Orders',
            ),
        ],
    )
    dcell_stream = [
        [
            DC('Detroit', liid_city, piid_city),
            DC('Furniture', liid_ctgry, piid_ctgry),
            DC(100, liid_sales, piid_sales),
            DC(10, liid_profit, piid_profit),
            DC(3, liid_customers, piid_customers),
            DC(4, liid_orders, piid_orders),
        ],
        [
            DC('Portland', liid_city, piid_city),
            DC('Furniture', liid_ctgry, piid_ctgry),
            DC(200, liid_sales, piid_sales),
            DC(20, liid_profit, piid_profit),
            DC(5, liid_customers, piid_customers),
            DC(6, liid_orders, piid_orders),
        ],
        [
            DC('Moscow', liid_city, piid_city),
            DC('Plants', liid_ctgry, piid_ctgry),
            DC(300, liid_sales, piid_sales),
            DC(30, liid_profit, piid_profit),
            DC(7, liid_customers, piid_customers),
            DC(10, liid_orders, piid_orders),
        ],
        [
            DC('Detroit', liid_city, piid_city),
            DC('Plants', liid_ctgry, piid_ctgry),
            DC(400, liid_sales, piid_sales),
            DC(40, liid_profit, piid_profit),
            DC(9, liid_customers, piid_customers),
            DC(23, liid_orders, piid_orders),
        ],
    ]
    transposed_stream = MeasureDataTransposer(
        dcell_stream=dcell_stream,
        pivot_legend=pivot_legend,
        cell_packer=JsonHashableValuePacker(),
    )
    transposed_data = list(transposed_stream)
    expected_data = [
        TransposedDataRow(
            dimensions=(
                DV(cells=(DC('Detroit', liid_city, piid_city),)),
                DV(cells=(DC('Furniture', liid_ctgry, piid_ctgry),)),
                DV(cells=(DC(MNV('Sales', piid_sales), liid_mnames, piid_mnames),)),
            ),
            value=DV(cells=(
                DC(100, liid_sales, piid_sales),
                DC(3, liid_customers, piid_customers),
            )),
        ),
        TransposedDataRow(
            dimensions=(
                DV(cells=(DC('Detroit', liid_city, piid_city),)),
                DV(cells=(DC('Furniture', liid_ctgry, piid_ctgry),)),
                DV(cells=(DC(MNV('Profit', piid_profit), liid_mnames, piid_mnames),)),
            ),
            value=DV(cells=(
                DC(10, liid_profit, piid_profit),
                DC(3, liid_customers, piid_customers),
                DC(4, liid_orders, piid_orders),
            )),
        ),
        TransposedDataRow(
            dimensions=(
                DV(cells=(DC('Portland', liid_city, piid_city),)),
                DV(cells=(DC('Furniture', liid_ctgry, piid_ctgry),)),
                DV(cells=(DC(MNV('Sales', piid_sales), liid_mnames, piid_mnames),)),
            ),
            value=DV(cells=(
                DC(200, liid_sales, piid_sales),
                DC(5, liid_customers, piid_customers),
            )),
        ),
        TransposedDataRow(
            dimensions=(
                DV(cells=(DC('Portland', liid_city, piid_city),)),
                DV(cells=(DC('Furniture', liid_ctgry, piid_ctgry),)),
                DV(cells=(DC(MNV('Profit', piid_profit), liid_mnames, piid_mnames),)),
            ),
            value=DV(cells=(
                DC(20, liid_profit, piid_profit),
                DC(5, liid_customers, piid_customers),
                DC(6, liid_orders, piid_orders),
            )),
        ),
        TransposedDataRow(
            dimensions=(
                DV(cells=(DC('Moscow', liid_city, piid_city),)),
                DV(cells=(DC('Plants', liid_ctgry, piid_ctgry),)),
                DV(cells=(DC(MNV('Sales', piid_sales), liid_mnames, piid_mnames),)),
            ),
            value=DV(cells=(
                DC(300, liid_sales, piid_sales),
                DC(7, liid_customers, piid_customers),
            )),
        ),
        TransposedDataRow(
            dimensions=(
                DV(cells=(DC('Moscow', liid_city, piid_city),)),
                DV(cells=(DC('Plants', liid_ctgry, piid_ctgry),)),
                DV(cells=(DC(MNV('Profit', piid_profit), liid_mnames, piid_mnames),)),
            ),
            value=DV(cells=(
                DC(30, liid_profit, piid_profit),
                DC(7, liid_customers, piid_customers),
                DC(10, liid_orders, piid_orders),
            )),
        ),
        TransposedDataRow(
            dimensions=(
                DV(cells=(DC('Detroit', liid_city, piid_city),)),
                DV(cells=(DC('Plants', liid_ctgry, piid_ctgry),)),
                DV(cells=(DC(MNV('Sales', piid_sales), liid_mnames, piid_mnames),)),
            ),
            value=DV(cells=(
                DC(400, liid_sales, piid_sales),
                DC(9, liid_customers, piid_customers),
            )),
        ),
        TransposedDataRow(
            dimensions=(
                DV(cells=(DC('Detroit', liid_city, piid_city),)),
                DV(cells=(DC('Plants', liid_ctgry, piid_ctgry),)),
                DV(cells=(DC(MNV('Profit', piid_profit), liid_mnames, piid_mnames),)),
            ),
            value=DV(cells=(
                DC(40, liid_profit, piid_profit),
                DC(9, liid_customers, piid_customers),
                DC(23, liid_orders, piid_orders)),
            ),
        ),
    ]
    assert transposed_data == expected_data


def test_data_cell_converter_with_dicts():
    liid_city = 0
    liid_value = 1
    legend_item_ids = (liid_city, liid_value)
    data = [
        MergedQueryDataRow(data=('Detroit', {'a': 1, 'f': 6}), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=('Portland', {'c': 2, 's': 7}), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=('Moscow', {'z': 3, 'q': {'some': 'thing'}}), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=('Detroit', {'y': 4, 'k': 100500}), legend_item_ids=legend_item_ids),
    ]

    legend = Legend(
        items=[
            LegendItem(
                legend_item_id=liid_city,
                role_spec=RowRoleSpec(role=FieldRole.row),
                obj=FieldObjSpec(id='1', title='City'),
                field_type=FieldType.DIMENSION,
                data_type=BIType.string,
            ),
        ],
    )

    piid_city = 10
    piid_value = 20
    pivot_legend = PivotLegend(
        items=[
            PivotLegendItem(
                pivot_item_id=piid_city, legend_item_ids=[liid_city],
                role_spec=PivotDimensionRoleSpec(role=PivotRole.pivot_row),
                title='City',
            ),
            PivotLegendItem(
                pivot_item_id=piid_value, legend_item_ids=[liid_value],
                role_spec=PivotMeasureRoleSpec(role=PivotRole.pivot_measure),
                title='Value',
            ),
        ],
    )

    dcell_stream = DataCellConverter(
        rows=data, cell_packer=JsonHashableValuePacker(),
        legend=legend, pivot_legend=pivot_legend,
    )
    converted_data = list(dcell_stream)
    expected_data = [
        [
            DC('Detroit', liid_city, piid_city),
            DC(JW('{"a": 1, "f": 6}'), liid_value, piid_value),
        ],
        [
            DC('Portland', liid_city, piid_city),
            DC(JW('{"c": 2, "s": 7}'), liid_value, piid_value),
        ],
        [
            DC('Moscow', liid_city, piid_city),
            DC(JW('{"q": {"some": "thing"}, "z": 3}'), liid_value, piid_value),
        ],
        [
            DC('Detroit', liid_city, piid_city),
            DC(JW('{"k": 100500, "y": 4}'), liid_value, piid_value),
        ],
    ]
    assert converted_data == expected_data
