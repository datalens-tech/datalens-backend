from collections import defaultdict
import logging
from typing import (
    TYPE_CHECKING,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Union,
)

import attr
import pandas as pd

from dl_api_lib.pivot.base.transformer import PivotTransformer
from dl_api_lib.pivot.empty.facade import EmptyDataFrameFacade
from dl_api_lib.pivot.pandas.facade import (
    PdDataFrameFacade,
    PdHSeriesDataFrameFacade,
    PdVSeriesDataFrameFacade,
)
from dl_api_lib.pivot.primitives import (
    DataCell,
    DataCellVector,
)
from dl_api_lib.pivot.stream_modifiers import (
    DataCellConverter,
    MeasureDataTransposer,
)
from dl_api_lib.pivot.table import PivotTable
from dl_constants.enums import PivotRole
import dl_query_processing.exc
from dl_query_processing.merging.primitives import MergedQueryDataRow


if TYPE_CHECKING:
    from dl_api_lib.pivot.base.facade import TableDataFacade
    from dl_api_lib.pivot.stream_modifiers import TransposedDataRow


LOGGER = logging.getLogger(__name__)


@attr.s
class PdPivotTransformer(PivotTransformer):
    """Created pandas-based pivot table"""

    def pivot(self, rows: Iterable[MergedQueryDataRow]) -> PivotTable:
        # Prepare data stream
        raw_dcell_stream: Iterable[Sequence[DataCell]] = DataCellConverter(
            rows=rows,
            cell_packer=self._cell_packer,
            legend=self._legend,
            pivot_legend=self._pivot_legend,
        )
        # Transpose measures (turn multiple measure fields into a single pseudo-field).
        transp_stream: Iterable[TransposedDataRow] = MeasureDataTransposer(
            dcell_stream=raw_dcell_stream,
            pivot_legend=self._pivot_legend,
            cell_packer=self._cell_packer,
        )

        fake_measure_piid_str = str(self._pivot_legend.get_unused_pivot_item_id())

        data: Dict[str, List[Optional[DataCellVector]]] = defaultdict(list)
        for dim_vectors, value_vector in transp_stream:
            for dim_vector in dim_vectors:
                base_dim_cell = dim_vector.cells[0]
                data[str(base_dim_cell.pivot_item_id)].append(dim_vector)
            data[fake_measure_piid_str].append(value_vector)

        any_data = bool(data)

        data_lengths = {len(values) for values in data.values()}
        if len(data_lengths) > 1:
            raise dl_query_processing.exc.PivotUnevenDataColumnsError(
                "Expected all data columns to contain the same number of values. " f"Got: {sorted(data_lengths)}"
            )

        raw_pd_df = pd.DataFrame(data)
        column_ids = [str(item.pivot_item_id) for item in self._pivot_legend.list_for_role(PivotRole.pivot_column)]
        row_ids = [str(item.pivot_item_id) for item in self._pivot_legend.list_for_role(PivotRole.pivot_row)]

        pd_pivot_res: Union[pd.DataFrame, pd.Series]
        facade: TableDataFacade

        pd_series: pd.Series
        pd_df: pd.DataFrame

        def wrapped_pivot(columns: Sequence[str], index: Sequence[str]) -> Union[pd.Series, pd.DataFrame]:
            try:
                return raw_pd_df.pivot(columns=columns, index=index)
            except ValueError as err:
                if str(err).startswith("Index contains duplicate entries"):
                    raise dl_query_processing.exc.PivotDuplicateDimensionValue() from err

                raise

        if not any_data:
            facade = EmptyDataFrameFacade(legend=self._legend, pivot_legend=self._pivot_legend)

        elif column_ids and row_ids:
            pd_pivot_res = wrapped_pivot(columns=column_ids, index=row_ids)
            assert isinstance(pd_pivot_res, pd.DataFrame)
            pd_df = pd_pivot_res
            # Drop the fake dimension `fake_measure_liid_str` from the column index:
            pd_df = pd_df.set_axis(axis="columns", labels=pd_df.columns.droplevel(0), copy=False)
            facade = PdDataFrameFacade(
                pd_df=pd_df,
                legend=self._legend,
                pivot_legend=self._pivot_legend,
            )

        elif column_ids:
            pd_pivot_res = wrapped_pivot(columns=column_ids, index=row_ids)
            assert isinstance(pd_pivot_res, pd.Series)
            pd_series = pd_pivot_res
            # Drop `fake_measure_liid_str` from the index:
            pd_series = pd_series.set_axis(labels=pd_series.index.droplevel(0), copy=False)
            facade = PdHSeriesDataFrameFacade(
                pd_series=pd_series,
                legend=self._legend,
                pivot_legend=self._pivot_legend,
            )

        elif row_ids:
            # Fool pandas into thinking columns are rows and vice-versa.
            # Otherwise it creates a DataFrame with an empty index for columns,
            # which does not behave the way a "normal" DataFrame should
            pd_pivot_res = wrapped_pivot(columns=row_ids, index=column_ids)
            assert isinstance(pd_pivot_res, pd.Series)
            pd_series = pd_pivot_res
            # Drop `fake_measure_liid_str` from the index:
            pd_series = pd_series.set_axis(labels=pd_series.index.droplevel(0), copy=False)
            facade = PdVSeriesDataFrameFacade(
                pd_series=pd_series,
                legend=self._legend,
                pivot_legend=self._pivot_legend,
            )

        else:
            raise TypeError("Invalid pivot configuration")

        LOGGER.info(f"Using {type(facade)} for pivot table")

        table = PivotTable(
            facade=facade,
            pivot_legend=self._pivot_legend,
            cell_packer=self._cell_packer,
        )
        return table
