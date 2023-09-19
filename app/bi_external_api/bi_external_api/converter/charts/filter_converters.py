import attr

from bi_external_api.converter.charts.ds_field_resolvers import MultiDatasetFieldResolver
from bi_external_api.converter.converter_exc import MalformedEntryConfig
from bi_external_api.domain import external as ext
from bi_external_api.domain.internal import charts
from dl_constants.enums import WhereClauseOperation


operation_map_ext_int: dict[ext.ComparisonOperation, WhereClauseOperation] = {
    ext.ComparisonOperation.IN: WhereClauseOperation.IN,
    ext.ComparisonOperation.NIN: WhereClauseOperation.NIN,
    ext.ComparisonOperation.EQ: WhereClauseOperation.EQ,
    ext.ComparisonOperation.NE: WhereClauseOperation.NE,
    ext.ComparisonOperation.GT: WhereClauseOperation.GT,
    ext.ComparisonOperation.LT: WhereClauseOperation.LT,
    ext.ComparisonOperation.GTE: WhereClauseOperation.GTE,
    ext.ComparisonOperation.LTE: WhereClauseOperation.LTE,
    ext.ComparisonOperation.ISNULL: WhereClauseOperation.ISNULL,
    ext.ComparisonOperation.ISNOTNULL: WhereClauseOperation.ISNOTNULL,
    ext.ComparisonOperation.ISTARTSWITH: WhereClauseOperation.ISTARTSWITH,
    ext.ComparisonOperation.STARTSWITH: WhereClauseOperation.STARTSWITH,
    ext.ComparisonOperation.IENDSWITH: WhereClauseOperation.IENDSWITH,
    ext.ComparisonOperation.ENDSWITH: WhereClauseOperation.ENDSWITH,
    ext.ComparisonOperation.ICONTAINS: WhereClauseOperation.ICONTAINS,
    ext.ComparisonOperation.CONTAINS: WhereClauseOperation.CONTAINS,
    ext.ComparisonOperation.NOTICONTAINS: WhereClauseOperation.NOTICONTAINS,
    ext.ComparisonOperation.NOTCONTAINS: WhereClauseOperation.NOTCONTAINS,
    ext.ComparisonOperation.BETWEEN: WhereClauseOperation.BETWEEN,
    ext.ComparisonOperation.LENEQ: WhereClauseOperation.LENEQ,
    ext.ComparisonOperation.LENGT: WhereClauseOperation.LENGT,
    ext.ComparisonOperation.LENGTE: WhereClauseOperation.LENGTE,
    ext.ComparisonOperation.LENLT: WhereClauseOperation.LENLT,
    ext.ComparisonOperation.LENLTE: WhereClauseOperation.LENLTE,
}

operation_map_int_ext: dict[WhereClauseOperation, ext.ComparisonOperation] = {
    v: k for (k, v) in operation_map_ext_int.items()
}


@attr.s()
class FilterExtToIntConverter:
    _dataset_field_resolver: MultiDatasetFieldResolver = attr.ib()

    def convert(
        self,
        ext_spec: ext.ChartFilter,
    ) -> charts.FieldFilter:
        dataset_name = ext_spec.field_ref.dataset_name
        if dataset_name is None:
            raise MalformedEntryConfig(f"Filter for field {ext_spec.field_ref.id} has no explicit dataset name")
        ds = self._dataset_field_resolver.get_dataset_by_name(name=dataset_name)
        chart_field = self._dataset_field_resolver.get_field_by_dataset_name_and_field_id(
            dataset_name=dataset_name, field_id=ext_spec.field_ref.id
        )
        field_filter = charts.Filter(
            value=charts.FilterValue(value=list(ext_spec.value.values)),
            operation=charts.FilterOperation(code=operation_map_ext_int[ext_spec.operation]),
        )
        return charts.FieldFilter(
            datasetId=ds.summary.id, filter=field_filter, **attr.asdict(chart_field, recurse=False)
        )


@attr.s()
class FilterIntToExtConverter:
    _dataset_id_to_name: dict[str, str] = attr.ib()

    def convert(self, int_config: charts.FieldFilter) -> ext.ChartFilter:
        return ext.ChartFilter(
            field_ref=ext.ChartFieldRef(
                id=int_config.guid, dataset_name=self._dataset_id_to_name[int_config.datasetId]
            ),
            operation=operation_map_int_ext[int_config.filter.operation.code],
            value=ext.MultiStringValue(int_config.filter.value.value),
        )
