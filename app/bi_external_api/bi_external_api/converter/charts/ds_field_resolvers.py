from typing import (
    Optional,
    Sequence,
)

import attr

from bi_external_api.converter.charts.utils import (
    ChartActionConverter,
    convert_field_type_dataset_to_chart,
)
from bi_external_api.converter.converter_exc import DatasetFieldNotFound
from bi_external_api.converter.workbook import WorkbookContext
from bi_external_api.domain import external as ext
from bi_external_api.domain.internal import (
    charts,
    datasets,
)
from bi_external_api.domain.internal.datasets import (
    ResultSchemaField,
    ResultSchemaFieldFull,
)
from dl_constants.enums import BIType


@attr.s()
class ModifiedDatasetFieldResolver:
    _ds: datasets.DatasetInstance = attr.ib()
    _ds_with_applied_updates: Optional[datasets.Dataset] = attr.ib()
    _chart_actions: Sequence[charts.ChartAction] = attr.ib()

    _map_field_id_add_action: dict[str, charts.ChartActionFieldAdd] = attr.ib(init=False, default=None)

    def __attrs_post_init__(self) -> None:
        self._map_field_id_add_action = {
            act.field.guid: act for act in self._chart_actions if isinstance(act, charts.ChartActionFieldAdd)
        }

    @classmethod
    def match_update_action_and_validation_result(
        cls,
        field_from_modified_dataset: ResultSchemaFieldFull,
        field_from_update_action: ResultSchemaField,
    ) -> None:
        to_cmp_from_modified_dataset: ResultSchemaField = field_from_modified_dataset.to_writable_result_schema()
        to_cmp_from_update_action: ResultSchemaField

        if field_from_update_action.guid_formula and field_from_update_action.formula:
            to_cmp_from_update_action = field_from_update_action
        elif field_from_update_action.guid_formula:
            # Action was converted as guid formula
            to_cmp_from_update_action = attr.evolve(field_from_update_action, formula=None)
            to_cmp_from_modified_dataset = attr.evolve(to_cmp_from_modified_dataset, formula=None)
        elif field_from_update_action.formula:
            # Action was converted as title formula
            to_cmp_from_update_action = attr.evolve(field_from_update_action, guid_formula=None)
            to_cmp_from_modified_dataset = attr.evolve(to_cmp_from_modified_dataset, guid_formula=None)
        else:
            # Action was converted direct fields
            to_cmp_from_update_action = attr.evolve(field_from_update_action, guid_formula=None, formula=None)
            to_cmp_from_modified_dataset = attr.evolve(to_cmp_from_modified_dataset, guid_formula=None, formula=None)

        if to_cmp_from_update_action != to_cmp_from_modified_dataset:
            raise AssertionError(
                f"Field from modified does not match field from action:"
                f" {to_cmp_from_modified_dataset} != {to_cmp_from_update_action}"
            )

    def get_chart_field(self, id: str) -> charts.ChartField:
        data_type: Optional[BIType]
        field_type: Optional[charts.DatasetFieldType]
        title: str

        if id in self._map_field_id_add_action:
            dataset_id, dataset_action = ChartActionConverter.convert_actions_add_field_chart_to_dataset(
                self._map_field_id_add_action[id]
            )
            assert self._ds.summary.id == dataset_id

            modified_dataset = self._ds_with_applied_updates
            # This is potential caveat in case if target database will not be reachable during conversion
            # In this case we will not be able to perform partial validation of chart
            assert (
                modified_dataset is not None
            ), f"Can not resolve ad-hoc field {id=}. Dataset was not provided to resolver"

            field_from_modified_dataset = modified_dataset.get_field_by_id(id)

            self.match_update_action_and_validation_result(field_from_modified_dataset, dataset_action.field)

            title = field_from_modified_dataset.title
            data_type = field_from_modified_dataset.data_type
            field_type = convert_field_type_dataset_to_chart(field_from_modified_dataset.type)
        else:
            dataset_field = self.get_dataset_field_by_id(id)

            title = dataset_field.title
            data_type = dataset_field.data_type
            field_type = convert_field_type_dataset_to_chart(dataset_field.type)

        return charts.ChartField(
            data_type=data_type,
            # originalTitle?: string;
            # fields?: V2Field[];
            type=field_type,
            title=title,
            guid=id,
            # formatting?: V2Formatting;
            # format: string;
            # labelMode: string;
            datasetId=self._ds.summary.id,
        )

    def get_dataset_field_by_id(self, id: str) -> ResultSchemaFieldFull:
        try:
            return self._ds.dataset.get_field_by_id(id)
        except StopIteration:
            raise DatasetFieldNotFound(f"id={id!r}")

    def get_ext_chart_field_with_ref_as_source(self, field_id: str) -> ext.ChartField:
        # TODO FIX: Add ability to skip verification
        if field_id not in self._map_field_id_add_action:
            # Ensuring that field actually exists in dataset
            self._ds.dataset.get_field_by_id(field_id)

        return ext.ChartField.create_as_ref(
            dataset_name=self._ds.summary.name,
            id=field_id,
        )

    def get_partials(self) -> Sequence[charts.DatasetFieldPartial]:
        rs_partials = [
            charts.DatasetFieldPartial(guid=rs_field.guid, title=rs_field.title)
            for rs_field in self._ds.dataset.result_schema
        ]
        updates_partials = [
            charts.DatasetFieldPartial(guid=act.field.guid, title=act.field.title)
            for act in self._chart_actions
            if isinstance(act, charts.ChartActionFieldAdd)
        ]
        return rs_partials + updates_partials


@attr.s()
class MultiDatasetFieldResolver:
    _wb_context: WorkbookContext = attr.ib()
    _actions: Sequence[charts.ChartAction] = attr.ib()
    _map_id_dataset_with_applied_actions: Optional[dict[str, datasets.Dataset]] = attr.ib()

    _single_dataset_resolver_cache_by_name: dict[str, ModifiedDatasetFieldResolver] = attr.ib(init=False, factory=dict)
    _single_dataset_resolver_cache_by_id: dict[str, ModifiedDatasetFieldResolver] = attr.ib(init=False, factory=dict)

    def _resolve_dataset_with_applied_actions(self, ds_id: str) -> Optional[datasets.Dataset]:
        ds_map = self._map_id_dataset_with_applied_actions
        if ds_map is not None:
            return ds_map.get(ds_id)
        return None

    def _get_single_dataset_resolver(
        self,
        *,
        name: Optional[str] = None,
        id: Optional[str] = None,
    ) -> ModifiedDatasetFieldResolver:
        ds: datasets.DatasetInstance

        if name is not None and id is None:
            if name in self._single_dataset_resolver_cache_by_name:
                return self._single_dataset_resolver_cache_by_name[name]

            ds = self._wb_context.resolve_dataset_by_name(name)
        elif id is not None and name is None:
            if id in self._single_dataset_resolver_cache_by_name:
                return self._single_dataset_resolver_cache_by_name[id]

            ds = self._wb_context.resolve_dataset_by_id(id)
        else:
            raise AssertionError("Only a single name or id must be provided")

        relevant_actions = [
            action
            for action in self._actions
            if isinstance(action, charts.ChartActionFieldAdd) and action.field.datasetId == ds.summary.id
        ]
        modified_dataset_resolver = ModifiedDatasetFieldResolver(
            ds=ds,
            chart_actions=relevant_actions,
            ds_with_applied_updates=self._resolve_dataset_with_applied_actions(ds.summary.id),
        )

        self._single_dataset_resolver_cache_by_name[ds.summary.name] = modified_dataset_resolver
        self._single_dataset_resolver_cache_by_id[ds.summary.id] = modified_dataset_resolver

        return modified_dataset_resolver

    @classmethod
    def is_measure_names(cls, field: charts.ChartField) -> bool:
        return field.type == charts.DatasetFieldType.PSEUDO and field.title == "Measure Names"

    def get_int_chart_field_by_ext_cf_source(self, ext_cf_src: ext.ChartFieldSource) -> charts.ChartField:
        if isinstance(ext_cf_src, ext.ChartFieldRef):
            return self._get_single_dataset_resolver(
                name=ext_cf_src.strict_dataset_name,
            ).get_chart_field(ext_cf_src.id)
        if isinstance(ext_cf_src, ext.MeasureNames):
            return charts.ChartField(
                type=charts.DatasetFieldType.PSEUDO,
                data_type=BIType.string,
                title="Measure Names",
            )
        if isinstance(ext_cf_src, ext.MeasureValues):
            return charts.ChartField(
                type=charts.DatasetFieldType.PSEUDO,
                data_type=BIType.float,
                title="Measure Values",
            )
        raise AssertionError("Unexpected type of chart field source")

    def get_ext_chart_field_by_int_field(self, int_chart_field: charts.ChartField) -> ext.ChartField:
        if int_chart_field.type == charts.DatasetFieldType.PSEUDO:
            try:
                return {
                    "Measure Names": ext.ChartField(source=ext.MeasureNames()),
                    "Measure Values": ext.ChartField(source=ext.MeasureValues()),
                }[int_chart_field.title]
            except KeyError:
                raise AssertionError(f"Can not determine pseudo field type {int_chart_field.title=}")

        field_id = int_chart_field.guid
        assert (
            field_id is not None
        ), "Got int_chart_field.guid=None in MultiDatasetFieldResolver.get_ext_chart_field_by_int_field()"

        return self._get_single_dataset_resolver(id=int_chart_field.datasetId).get_ext_chart_field_with_ref_as_source(
            field_id
        )

    def get_partials(self, dataset_name: str) -> Sequence[charts.DatasetFieldPartial]:
        return self._get_single_dataset_resolver(name=dataset_name).get_partials()

    def get_dataset_by_name(self, name: str) -> datasets.DatasetInstance:
        return self._wb_context.resolve_dataset_by_name(name)

    def get_field_by_dataset_name_and_field_id(self, dataset_name: str, field_id: str) -> ResultSchemaFieldFull:
        return self._get_single_dataset_resolver(name=dataset_name).get_dataset_field_by_id(field_id)
