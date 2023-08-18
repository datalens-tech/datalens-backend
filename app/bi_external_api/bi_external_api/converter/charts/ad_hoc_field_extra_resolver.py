from typing import Mapping, Optional, Sequence

import attr

from bi_external_api.converter.charts.utils import ChartActionConverter, convert_field_type_dataset_to_chart
from bi_external_api.converter.workbook import WorkbookContext
from bi_external_api.domain.internal import datasets, charts
from bi_external_api.domain.internal.charts import ChartAction
from bi_external_api.internal_api_clients.dataset_api import APIClientBIBackControlPlane


@attr.s()
class AdHocFieldExtraResolver:
    """
    Currently, there are some ad-hoc field properties in chart configuration,
     that can be resolved only via dataset API calls (e.g. formula field data type).
    This class is responsible for resolution of such props after base conversion.
    """
    _wb_context: WorkbookContext = attr.ib()
    _bi_api_client: APIClientBIBackControlPlane = attr.ib()

    @staticmethod
    def ensure_chart_field_data_type_and_type(
            dataset_id_map: dict[str, datasets.Dataset],
            cf: charts.ChartField
    ) -> charts.ChartField:
        if cf.data_type is not None or cf.type == charts.DatasetFieldType.PSEUDO:
            return cf

        dataset_id = cf.datasetId
        field_id = cf.guid
        assert dataset_id is not None, \
            "Got cf.datasetId=None in AdHocFieldExtraResolver.ensure_chart_field_data_type_and_type()"
        assert field_id is not None, \
            "Got cf.guid=None in AdHocFieldExtraResolver.ensure_chart_field_data_type_and_type()"

        ds = dataset_id_map[dataset_id]
        # TODO FIX: Handle unknown IDs
        rs_field = ds.get_field_by_id(field_id)

        return attr.evolve(
            cf,
            data_type=rs_field.data_type,
            type=convert_field_type_dataset_to_chart(rs_field.type)
        )

    async def resolve_updates(
            self,
            updates: Optional[Sequence[ChartAction]],
            map_ds_id_to_source_actions: Optional[Mapping[str, Sequence[datasets.Action]]] = None,
    ) -> dict[str, datasets.Dataset]:
        if updates is None or len(updates) == 0:
            return {}

        action_tuples: list[tuple[str, datasets.ActionFieldAdd]] = []

        for act in updates:
            assert isinstance(act, charts.ChartActionFieldAdd)
            action_tuples.append(ChartActionConverter.convert_actions_add_field_chart_to_dataset(act))

        affected_datasets = list(sorted(set(ds_id for ds_id, _ in action_tuples)))
        map_dataset_id_modified_dataset: dict[str, datasets.Dataset] = {}

        for current_ds_id in affected_datasets:
            effective_dataset_id: Optional[str]
            effective_dataset_actions: list[datasets.Action]

            actions_from_chart: list[datasets.Action] = [
                action for ds_id, action in action_tuples if ds_id == current_ds_id
            ]

            if map_ds_id_to_source_actions is not None and current_ds_id in map_ds_id_to_source_actions:
                effective_dataset_id = None

                dataset_actions = map_ds_id_to_source_actions[current_ds_id]
                dataset_fields_count = len([act for act in dataset_actions if isinstance(act, datasets.ActionFieldAdd)])

                effective_dataset_actions = [
                    *dataset_actions,
                    *(
                        attr.evolve(
                            act,
                            order_index=dataset_fields_count + idx
                        ) for idx, act in enumerate(actions_from_chart)
                    ),
                ]
            else:
                effective_dataset_id = current_ds_id
                effective_dataset_actions = [
                    attr.evolve(
                        act,
                        order_index=0
                    ) for act in actions_from_chart
                ]

            modified_dataset, _ = await self._bi_api_client.build_dataset_config_by_actions(
                effective_dataset_actions,
                dataset_id=effective_dataset_id,
            )
            map_dataset_id_modified_dataset[current_ds_id] = modified_dataset

        return map_dataset_id_modified_dataset
