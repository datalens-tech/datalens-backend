import pytest

import bi_query_processing.exc
from bi_constants.enums import BIType, FieldType, PivotRole, PivotItemType

from bi_api_lib.query.formalization.pivot_legend import (
    PivotLegend, PivotLegendItem, PivotMeasureRoleSpec, PivotDimensionRoleSpec,
)
from bi_query_processing.legend.field_legend import (
    Legend, LegendItem, FieldObjSpec, MeasureNameObjSpec,
)
from bi_api_lib.pivot.primitives import (
    DataCell as DC, DataCellVector as DV, DataRow, MeasureNameValue as MNV, PivotHeader
)
from bi_api_lib.pivot.table import PivotTable
from bi_api_lib.pivot.pandas.transformer import PdPivotTransformer
from bi_query_processing.merging.primitives import MergedQueryDataRow


def test_pivot_empty_data():
    fid_city, fid_ctgry, fid_sales = ('123', '456', '789')
    data = []

    # Legend item IDs
    liid_ctgry, liid_city, liid_sales = (0, 1, 2)
    legend = Legend(items=[
        LegendItem(
            legend_item_id=liid_ctgry,
            obj=FieldObjSpec(id=fid_ctgry, title='Category'),
            field_type=FieldType.DIMENSION, data_type=BIType.string,
        ),
        LegendItem(
            legend_item_id=liid_city,
            obj=FieldObjSpec(id=fid_city, title='City'),
            field_type=FieldType.DIMENSION, data_type=BIType.string,
        ),
        LegendItem(
            legend_item_id=liid_sales,
            obj=FieldObjSpec(id=fid_sales, title='Sales'),
            field_type=FieldType.MEASURE, data_type=BIType.integer,
        ),
    ])

    # Pivot item IDs
    piid_city = 10
    piid_ctgry = 20
    piid_sales = 30
    pivot_legend = PivotLegend(
        items=[
            PivotLegendItem(
                pivot_item_id=piid_ctgry, legend_item_ids=[liid_ctgry],
                role_spec=PivotDimensionRoleSpec(role=PivotRole.pivot_column),
                title='Category',
            ),
            PivotLegendItem(
                pivot_item_id=piid_city, legend_item_ids=[liid_city],
                role_spec=PivotDimensionRoleSpec(role=PivotRole.pivot_row),
                title='City',
            ),
            PivotLegendItem(
                pivot_item_id=piid_sales, legend_item_ids=[liid_sales],
                role_spec=PivotMeasureRoleSpec(role=PivotRole.pivot_measure),
                title='Sales',
            ),
        ],
    )

    transformer = PdPivotTransformer(legend=legend, pivot_legend=pivot_legend)
    actual_pivot_table = transformer.pivot(data)

    expected_pivot_columns = []
    actual_pivot_columns = actual_pivot_table.get_columns()
    assert actual_pivot_columns == expected_pivot_columns

    expected_pivot_rows = []
    actual_pivot_rows = list(actual_pivot_table.get_rows())
    assert actual_pivot_rows == expected_pivot_rows


def test_pivot_duplicate_dimension_values():
    fid_city, fid_ctgry, fid_sales = ('123', '456', '789')
    liid_ctgry, liid_city, liid_sales = (0, 1, 2)
    legend_item_ids = (liid_ctgry, liid_city, liid_sales)

    data = [
        MergedQueryDataRow(data=('Detroit', 'Furniture', 100), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=('San Francisco', 'Furniture', 200), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=('Moscow', 'Office Supplies', 300), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=('Detroit', 'Furniture', 400), legend_item_ids=legend_item_ids),
    ]

    legend = Legend(items=[
        LegendItem(
            legend_item_id=liid_ctgry,
            obj=FieldObjSpec(id=fid_ctgry, title='Category'),
            field_type=FieldType.DIMENSION, data_type=BIType.string,
        ),
        LegendItem(
            legend_item_id=liid_city,
            obj=FieldObjSpec(id=fid_city, title='City'),
            field_type=FieldType.DIMENSION, data_type=BIType.string,
        ),
        LegendItem(
            legend_item_id=liid_sales,
            obj=FieldObjSpec(id=fid_sales, title='Sales'),
            field_type=FieldType.MEASURE, data_type=BIType.integer,
        ),
    ])

    # Pivot item IDs
    piid_city = 10
    piid_ctgry = 20
    piid_sales = 30
    pivot_legend = PivotLegend(
        items=[
            PivotLegendItem(
                pivot_item_id=piid_ctgry, legend_item_ids=[liid_ctgry],
                role_spec=PivotDimensionRoleSpec(role=PivotRole.pivot_column),
                title='Category',
            ),
            PivotLegendItem(
                pivot_item_id=piid_city, legend_item_ids=[liid_city],
                role_spec=PivotDimensionRoleSpec(role=PivotRole.pivot_row),
                title='City',
            ),
            PivotLegendItem(
                pivot_item_id=piid_sales, legend_item_ids=[liid_sales],
                role_spec=PivotMeasureRoleSpec(role=PivotRole.pivot_measure),
                title='Sales',
            ),
        ],
    )

    transformer = PdPivotTransformer(legend=legend, pivot_legend=pivot_legend)
    with pytest.raises(bi_query_processing.exc.PivotDuplicateDimensionValue):
        transformer.pivot(data)


def _check_pagination(pivot_table: PivotTable, offset_rows: int = 1, limit_rows: int = 3) -> None:
    initial_columns = pivot_table.get_columns()
    initial_rows = list(pivot_table.get_rows())

    pivot_table.facade.paginate(offset_rows=offset_rows, limit_rows=limit_rows)

    paginated_columns = list(pivot_table.get_columns())
    paginated_rows = list(pivot_table.get_rows())

    assert paginated_columns == initial_columns

    if pivot_table.get_row_count() == 1:
        # A Dummy row (filled with None) is returned in this case
        assert paginated_rows == [DataRow(
            header=PivotHeader(),
            values=tuple(None for _ in range(len(paginated_columns)))
        )]
    else:
        assert paginated_rows == initial_rows[offset_rows:offset_rows + limit_rows]


def test_pivot_only_row_dims_multiple_measures():
    fid_city, fid_sales, fid_profit = ('123', '456', '789')
    liid_city, liid_mnames, liid_sales, liid_profit = (0, 1, 2, 3)
    legend_item_ids = (liid_city, liid_sales, liid_profit)

    data = [
        MergedQueryDataRow(data=('Detroit', 100, 10), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=('San Francisco', 200, 20), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=('Moscow', 300, 30), legend_item_ids=legend_item_ids),
    ]

    legend = Legend(items=[
        LegendItem(
            legend_item_id=liid_city,
            obj=FieldObjSpec(id=fid_city, title='City'),
            field_type=FieldType.DIMENSION, data_type=BIType.string,
        ),
        LegendItem(
            legend_item_id=liid_mnames,
            obj=MeasureNameObjSpec(),
            field_type=FieldType.DIMENSION, data_type=BIType.string,
        ),
        LegendItem(
            legend_item_id=liid_sales,
            obj=FieldObjSpec(id=fid_sales, title='Sales'),
            field_type=FieldType.MEASURE, data_type=BIType.integer,
        ),
        LegendItem(
            legend_item_id=liid_profit,
            obj=FieldObjSpec(id=fid_profit, title='Profit'),
            field_type=FieldType.MEASURE, data_type=BIType.integer,
        ),
    ])

    # Pivot item IDs
    piid_city = 10
    piid_mnames = 20
    piid_sales = 30
    piid_profit = 40
    pivot_legend = PivotLegend(
        items=[
            PivotLegendItem(
                pivot_item_id=piid_city, legend_item_ids=[liid_city],
                role_spec=PivotDimensionRoleSpec(role=PivotRole.pivot_row),
                title='City',
            ),
            PivotLegendItem(
                pivot_item_id=piid_mnames, item_type=PivotItemType.measure_name, legend_item_ids=[liid_mnames],
                role_spec=PivotDimensionRoleSpec(role=PivotRole.pivot_row),
                title='Measure Name',
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

    transformer = PdPivotTransformer(legend=legend, pivot_legend=pivot_legend)
    actual_pivot_table = transformer.pivot(data)

    # Sort it
    actual_pivot_table.facade.sort()

    expected_pivot_columns = [
        PivotHeader()
    ]
    actual_pivot_columns = actual_pivot_table.get_columns()
    assert actual_pivot_columns == expected_pivot_columns

    expected_pivot_rows = [
        DataRow(
            header=PivotHeader(values=(
                DV(cells=(DC('Detroit', liid_city, piid_city),)),
                DV(cells=(DC(MNV('Sales', piid_sales), liid_mnames, piid_mnames),)),
            )),
            values=(
                DV(cells=(DC(100, liid_sales, piid_sales),)),
            ),
        ),
        DataRow(
            header=PivotHeader(values=(
                DV(cells=(DC('Detroit', liid_city, piid_city),)),
                DV(cells=(DC(MNV('Profit', piid_profit), liid_mnames, piid_mnames),)),
            )),
            values=(
                DV(cells=(DC(10, liid_profit, piid_profit),)),
            ),
        ),
        DataRow(
            header=PivotHeader(values=(
                DV(cells=(DC('Moscow', liid_city, piid_city),)),
                DV(cells=(DC(MNV('Sales', piid_sales), liid_mnames, piid_mnames),)),
            )),
            values=(
                DV(cells=(DC(300, liid_sales, piid_sales),)),
            ),
        ),
        DataRow(
            header=PivotHeader(values=(
                DV(cells=(DC('Moscow', liid_city, piid_city),)),
                DV(cells=(DC(MNV('Profit', piid_profit), liid_mnames, piid_mnames),)),
            )),
            values=(
                DV(cells=(DC(30, liid_profit, piid_profit),)),
            ),
        ),
        DataRow(
            header=PivotHeader(values=(
                DV(cells=(DC('San Francisco', liid_city, piid_city),)),
                DV(cells=(DC(MNV('Sales', piid_sales), liid_mnames, piid_mnames),)),
            )),
            values=(
                DV(cells=(DC(200, liid_sales, piid_sales),)),
            ),
        ),
        DataRow(
            header=PivotHeader(values=(
                DV(cells=(DC('San Francisco', liid_city, piid_city),)),
                DV(cells=(DC(MNV('Profit', piid_profit), liid_mnames, piid_mnames),)),
            )),
            values=(
                DV(cells=(DC(20, liid_profit, piid_profit),)),
            ),
        ),
    ]
    actual_pivot_rows = list(actual_pivot_table.get_rows())
    assert actual_pivot_rows == expected_pivot_rows

    _check_pagination(actual_pivot_table)


def test_pivot_only_column_dims_multiple_measures():
    fid_city, fid_sales, fid_profit = ('123', '456', '789')
    liid_city, liid_mnames, liid_sales, liid_profit = (0, 1, 2, 3)
    legend_item_ids = (liid_city, liid_sales, liid_profit)

    data = [
        MergedQueryDataRow(data=('Detroit', 100, 10), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=('San Francisco', 200, 20), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=('Moscow', 300, 30), legend_item_ids=legend_item_ids),
    ]

    legend = Legend(items=[
        LegendItem(
            legend_item_id=liid_city,
            obj=FieldObjSpec(id=fid_city, title='City'),
            field_type=FieldType.DIMENSION, data_type=BIType.string,
        ),
        LegendItem(
            legend_item_id=liid_mnames,
            obj=MeasureNameObjSpec(),
            field_type=FieldType.DIMENSION, data_type=BIType.string,
        ),
        LegendItem(
            legend_item_id=liid_sales,
            obj=FieldObjSpec(id=fid_sales, title='Sales'),
            field_type=FieldType.MEASURE, data_type=BIType.integer,
        ),
        LegendItem(
            legend_item_id=liid_profit,
            obj=FieldObjSpec(id=fid_profit, title='Profit'),
            field_type=FieldType.MEASURE, data_type=BIType.integer,
        ),
    ])

    # Pivot item IDs
    piid_city = 10
    piid_mnames = 20
    piid_sales = 30
    piid_profit = 40
    pivot_legend = PivotLegend(
        items=[
            PivotLegendItem(
                pivot_item_id=piid_city, legend_item_ids=[liid_city],
                role_spec=PivotDimensionRoleSpec(role=PivotRole.pivot_column),
                title='City',
            ),
            PivotLegendItem(
                pivot_item_id=piid_mnames, item_type=PivotItemType.measure_name, legend_item_ids=[liid_mnames],
                role_spec=PivotDimensionRoleSpec(role=PivotRole.pivot_column),
                title='Measure Name',
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

    # Pivot
    transformer = PdPivotTransformer(legend=legend, pivot_legend=pivot_legend)
    actual_pivot_table = transformer.pivot(data)

    # Sort it
    actual_pivot_table.facade.sort()

    expected_pivot_columns = [
        PivotHeader(values=(
            DV(cells=(DC('Detroit', liid_city, piid_city),)),
            DV(cells=(DC(MNV('Sales', piid_sales), liid_mnames, piid_mnames),)),
        )),
        PivotHeader(values=(
            DV(cells=(DC('Detroit', liid_city, piid_city),)),
            DV(cells=(DC(MNV('Profit', piid_profit), liid_mnames, piid_mnames),)),
        )),
        PivotHeader(values=(
            DV(cells=(DC('Moscow', liid_city, piid_city),)),
            DV(cells=(DC(MNV('Sales', piid_sales), liid_mnames, piid_mnames),)),
        )),
        PivotHeader(values=(
            DV(cells=(DC('Moscow', liid_city, piid_city),)),
            DV(cells=(DC(MNV('Profit', piid_profit), liid_mnames, piid_mnames),)),
        )),
        PivotHeader(values=(
            DV(cells=(DC('San Francisco', liid_city, piid_city),)),
            DV(cells=(DC(MNV('Sales', piid_sales), liid_mnames, piid_mnames),)),
        )),
        PivotHeader(values=(
            DV(cells=(DC('San Francisco', liid_city, piid_city),)),
            DV(cells=(DC(MNV('Profit', piid_profit), liid_mnames, piid_mnames),)),
        )),
    ]
    actual_pivot_columns = actual_pivot_table.get_columns()
    assert actual_pivot_columns == expected_pivot_columns

    expected_pivot_rows = [
        DataRow(
            header=PivotHeader(),
            values=(
                DV(cells=(DC(100, liid_sales, piid_sales),)),
                DV(cells=(DC(10, liid_profit, piid_profit),)),
                DV(cells=(DC(300, liid_sales, piid_sales),)),
                DV(cells=(DC(30, liid_profit, piid_profit),)),
                DV(cells=(DC(200, liid_sales, piid_sales),)),
                DV(cells=(DC(20, liid_profit, piid_profit),)),
            ),
        ),
    ]
    actual_pivot_rows = list(actual_pivot_table.get_rows())
    assert actual_pivot_rows == expected_pivot_rows

    # TODO: Check pagination


def test_pivot_only_row_dims_no_measures():
    fid_city = '123'
    liid_city = 0
    legend_item_ids = (liid_city,)

    data = [
        MergedQueryDataRow(data=('Detroit',), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=('San Francisco',), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=('Moscow',), legend_item_ids=legend_item_ids),
    ]

    legend = Legend(items=[
        LegendItem(
            legend_item_id=liid_city,
            obj=FieldObjSpec(id=fid_city, title='City'),
            field_type=FieldType.DIMENSION, data_type=BIType.string,
        ),
    ])

    # Pivot item IDs
    piid_city = 10
    pivot_legend = PivotLegend(
        items=[
            PivotLegendItem(
                pivot_item_id=piid_city, legend_item_ids=[liid_city],
                role_spec=PivotDimensionRoleSpec(role=PivotRole.pivot_row),
                title='City',
            ),
        ],
    )

    transformer = PdPivotTransformer(legend=legend, pivot_legend=pivot_legend)
    actual_pivot_table = transformer.pivot(data)

    # Sort it
    actual_pivot_table.facade.sort()

    expected_pivot_columns = [PivotHeader()]  # One pseudo-column with empty header (no dims)
    actual_pivot_columns = actual_pivot_table.get_columns()
    assert actual_pivot_columns == expected_pivot_columns

    expected_pivot_rows = [
        DataRow(
            header=PivotHeader(values=(DV(cells=(DC('Detroit', liid_city, piid_city),)),)),
            values=(None,),
        ),
        DataRow(
            header=PivotHeader(values=(DV(cells=(DC('Moscow', liid_city, piid_city),)),)),
            values=(None,),
        ),
        DataRow(
            header=PivotHeader(values=(DV(cells=(DC('San Francisco', liid_city, piid_city),)),)),
            values=(None,),
        ),
    ]
    actual_pivot_rows = list(actual_pivot_table.get_rows())
    assert actual_pivot_rows == expected_pivot_rows

    _check_pagination(actual_pivot_table)


def test_pivot_only_column_dims_no_measures():
    fid_city = '123'
    liid_city = 0
    legend_item_ids = (liid_city,)

    data = [
        MergedQueryDataRow(data=('Detroit',), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=('San Francisco',), legend_item_ids=legend_item_ids),
        MergedQueryDataRow(data=('Moscow',), legend_item_ids=legend_item_ids),
    ]

    # Legend item IDs
    legend = Legend(items=[
        LegendItem(
            legend_item_id=liid_city,
            obj=FieldObjSpec(id=fid_city, title='City'),
            field_type=FieldType.DIMENSION, data_type=BIType.string,
        ),
    ])

    # Pivot item IDs
    piid_city = 10
    pivot_legend = PivotLegend(
        items=[
            PivotLegendItem(
                pivot_item_id=piid_city, legend_item_ids=[liid_city],
                role_spec=PivotDimensionRoleSpec(role=PivotRole.pivot_column),
                title='City',
            ),
        ],
    )

    transformer = PdPivotTransformer(legend=legend, pivot_legend=pivot_legend)
    actual_pivot_table = transformer.pivot(data)

    # Sort it
    actual_pivot_table.facade.sort()

    expected_pivot_columns = [
        PivotHeader(values=(DV(cells=(DC('Detroit', liid_city, piid_city),)),)),
        PivotHeader(values=(DV(cells=(DC('Moscow', liid_city, piid_city),)),)),
        PivotHeader(values=(DV(cells=(DC('San Francisco', liid_city, piid_city),)),)),
    ]
    actual_pivot_columns = actual_pivot_table.get_columns()
    assert actual_pivot_columns == expected_pivot_columns

    expected_pivot_rows = [
        DataRow(  # One pseudo-row
            header=PivotHeader(),  # with no row-level dimensions
            values=(None, None, None),  # and values for pseudo-measure
        ),
    ]
    actual_pivot_rows = list(actual_pivot_table.get_rows())
    assert actual_pivot_rows == expected_pivot_rows

    _check_pagination(actual_pivot_table)
