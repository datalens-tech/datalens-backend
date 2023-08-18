from typing import Any, Iterable, TypeVar, Type, Mapping

import attr

from bi_external_api.domain import external as ext
from bi_external_api.workbook_ops.diff_tools import EntryInstanceAction, WorkbookTransitionPlan, ActualInstanceInfo
from bi_external_api.workbook_ops.wb_accessor import WorkbookAccessor

_ENTRY_INSTANCE_TV = TypeVar("_ENTRY_INSTANCE_TV", bound=ext.EntryInstance)


@attr.s(frozen=True)
class ExtWorkbookDiffCalculator:
    wb: ext.WorkBook = attr.ib()
    map_inst_clz_broken_entry_name_set: Mapping[Type[ext.EntryInstance], set[str]] = attr.ib()
    do_force_rewrite: bool = attr.ib(default=False)

    @staticmethod
    def get_data_for_instance(instance: ext.EntryInstance) -> Any:
        if isinstance(instance, ext.DatasetInstance):
            return instance.dataset
        elif isinstance(instance, ext.ChartInstance):
            return instance.chart
        elif isinstance(instance, ext.DashInstance):
            return instance.dashboard

        raise AssertionError(f"Unknown type of external entry instance {type(instance)=}")

    def are_instances_equals(self, a: _ENTRY_INSTANCE_TV, b: _ENTRY_INSTANCE_TV) -> bool:
        assert a.name == b.name
        return self.get_data_for_instance(a) == self.get_data_for_instance(b)

    def get_actual_instances(self, entry_clz: Type[_ENTRY_INSTANCE_TV]) -> Iterable[_ENTRY_INSTANCE_TV]:
        return WorkbookAccessor(self.wb).get_actual_instances(entry_clz)

    def calculate_transition_actions(
            self,
            inst_clz: Type[_ENTRY_INSTANCE_TV],
            desired_set: Iterable[_ENTRY_INSTANCE_TV],
    ) -> Iterable[EntryInstanceAction[_ENTRY_INSTANCE_TV]]:
        actual_map = {
            inst.name: inst
            for inst in self.get_actual_instances(inst_clz)
        }
        actual_broken_entry_name_set = self.map_inst_clz_broken_entry_name_set[inst_clz]

        all_actual_names = actual_map.keys() | actual_broken_entry_name_set

        desired_map = {inst.name: inst for inst in desired_set}

        ret = []

        # Collecting instances that appears only in desired set
        for name in desired_map.keys() - all_actual_names:
            ret.append(EntryInstanceAction(
                name,
                actual_instance_info=None,
                desired_instance=desired_map[name],
            ))

        # Checking instances that appears in both desired & actual set
        for name in desired_map.keys() & all_actual_names:
            desired_instance = desired_map[name]
            actual_instance_to_modify_info: ActualInstanceInfo

            if name in actual_map:
                actual_instance = actual_map[name]
                if not self.do_force_rewrite and self.are_instances_equals(actual_instance, desired_instance):
                    continue

                actual_instance_to_modify_info = ActualInstanceInfo(
                    instance=actual_map[name],
                    load_fail_reason=None,
                )
            else:
                # Entry is broken
                actual_instance_to_modify_info = ActualInstanceInfo(
                    instance=None,
                    load_fail_reason="Something went wrong",
                )

            ret.append(EntryInstanceAction(
                name,
                actual_instance_info=actual_instance_to_modify_info,
                desired_instance=desired_map[name],
            ))

        # Collecting instances that appears only in actual set
        for name in all_actual_names - desired_map.keys():
            actual_instance_to_delete: ActualInstanceInfo
            if name in actual_map:
                actual_instance_to_delete = ActualInstanceInfo(instance=actual_map[name], load_fail_reason=None)
            else:
                actual_instance_to_delete = ActualInstanceInfo(instance=None, load_fail_reason="Something went wrong")

            ret.append(EntryInstanceAction(
                name,
                actual_instance_info=actual_instance_to_delete,
                desired_instance=None,
            ))

        return ret

    def get_workbook_transition_plan(self, desired_workbook: ext.WorkBook) -> WorkbookTransitionPlan:
        # TODO FIX: Check name uniqueness across all instance classes
        # TODO FIX: Take in account situations when name change class during transition
        dataset_transition_actions = self.calculate_transition_actions(
            ext.DatasetInstance,
            desired_workbook.datasets,
        )
        charts_transition_actions = self.calculate_transition_actions(
            ext.ChartInstance,
            desired_workbook.charts
        )
        dash_transition_actions = self.calculate_transition_actions(
            ext.DashInstance,
            desired_workbook.dashboards
        )
        return WorkbookTransitionPlan(
            dataset_actions=dataset_transition_actions,
            chart_actions=charts_transition_actions,
            dash_actions=dash_transition_actions,
        )
