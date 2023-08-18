import enum
from typing import Optional, TypeVar, Generic, Iterable, Type, Sequence

import attr

from bi_external_api.domain import external as ext

_EXT_WB_INST_TV = TypeVar("_EXT_WB_INST_TV", bound=ext.EntryInstance)


class EntryActionKind(enum.Enum):
    create = enum.auto()
    modify = enum.auto()
    delete = enum.auto()


@attr.s(frozen=True)
class ActualInstanceInfo(Generic[_EXT_WB_INST_TV]):
    instance: Optional[_EXT_WB_INST_TV] = attr.ib()
    load_fail_reason: Optional[str] = attr.ib()


@attr.s(frozen=True)
class EntryInstanceAction(Generic[_EXT_WB_INST_TV]):
    entry_name: str = attr.ib()
    actual_instance_info: Optional[ActualInstanceInfo[_EXT_WB_INST_TV]] = attr.ib()
    desired_instance: Optional[_EXT_WB_INST_TV] = attr.ib()

    def __attrs_post_init__(self) -> None:
        actual_instance_info = self.actual_instance_info

        if actual_instance_info is None and self.desired_instance is None:
            raise AssertionError("Actual & desired instances can not be None simultaneously")
        if actual_instance_info is not None and self.desired_instance is not None:
            actual_instance = actual_instance_info.instance
            if actual_instance is not None:
                assert type(actual_instance) == type(self.desired_instance)  # noqa: E721

        if actual_instance_info is not None and actual_instance_info.instance is not None:
            actual_instance = actual_instance_info.instance
            if actual_instance is not None:
                assert actual_instance.name == self.entry_name

        if self.desired_instance is not None:
            assert self.desired_instance.name == self.entry_name

    @property
    def kind(self) -> EntryActionKind:
        if self.actual_instance_info is None and self.desired_instance is not None:
            return EntryActionKind.create
        if self.actual_instance_info is not None and self.desired_instance is None:
            return EntryActionKind.delete
        if self.actual_instance_info is not None and self.desired_instance is not None:
            return EntryActionKind.modify
        raise AssertionError("Actual instance info & desired instances can not be None simultaneously")

    @property
    def has_actual_instance(self) -> bool:
        return self.actual_instance_info is not None and self.actual_instance_info.instance is not None

    @property
    def actual_instance_strict(self) -> _EXT_WB_INST_TV:
        actual_inst_info = self.actual_instance_info
        assert actual_inst_info is not None
        inst = actual_inst_info.instance
        assert inst is not None
        return inst

    @property
    def desired_instance_strict(self) -> _EXT_WB_INST_TV:
        inst = self.desired_instance
        assert inst is not None
        return inst


@attr.s(frozen=True)
class WorkbookTransitionPlan:
    dataset_actions: Iterable[EntryInstanceAction[ext.DatasetInstance]] = attr.ib()
    chart_actions: Iterable[EntryInstanceAction[ext.ChartInstance]] = attr.ib()
    dash_actions: Iterable[EntryInstanceAction[ext.DashInstance]] = attr.ib()

    @classmethod
    def _filter_actions(
            cls,
            raw: Iterable[EntryInstanceAction[_EXT_WB_INST_TV]],
            *,
            kind: Optional[EntryActionKind],
    ) -> Iterable[EntryInstanceAction[_EXT_WB_INST_TV]]:
        if kind is None:
            return raw
        return [act for act in raw if act.kind == kind]

    def _get_actions(
            self,
            clz: Type[_EXT_WB_INST_TV],
            action_kind: Optional[EntryActionKind],
    ) -> Iterable[EntryInstanceAction[_EXT_WB_INST_TV]]:
        if clz is ext.DatasetInstance:
            # TODO FIX: Use parametrized methods WB CTX to get instances
            return self._filter_actions(self.dataset_actions, kind=action_kind)  # type: ignore
        if clz is ext.ChartInstance:
            return self._filter_actions(self.chart_actions, kind=action_kind)  # type: ignore
        if clz is ext.DashInstance:
            return self._filter_actions(self.dash_actions, kind=action_kind)  # type: ignore
        # TODO FIX: Clarify message
        raise AssertionError()

    def get_items_to_create(self, clz: Type[_EXT_WB_INST_TV]) -> Iterable[_EXT_WB_INST_TV]:
        return [
            act.desired_instance_strict
            for act in self._get_actions(clz, action_kind=EntryActionKind.create)
        ]

    def get_item_names_to_delete(self, clz: Type[_EXT_WB_INST_TV]) -> Iterable[str]:
        return [
            act.entry_name
            for act in self._get_actions(clz, action_kind=EntryActionKind.delete)
        ]

    def get_items_to_rewrite(self, clz: Type[_EXT_WB_INST_TV]) -> Iterable[_EXT_WB_INST_TV]:
        return [
            act.desired_instance_strict
            for act in self._get_actions(clz, action_kind=EntryActionKind.modify)
        ]

    def to_external(self) -> ext.ModificationPlan:
        ret: list[ext.EntryOperation] = []
        all_instance_types: Sequence[Type[ext.EntryInstance]] = (
            ext.DatasetInstance,
            ext.ChartInstance,
            ext.DashInstance,
        )
        entry_action: EntryInstanceAction

        for clz in all_instance_types:
            ret.extend([
                ext.EntryOperation(
                    entry_name=entry_action.entry_name,
                    entry_kind=clz.kind,
                    operation_kind={
                        EntryActionKind.create: ext.EntryOperationKind.create,
                        EntryActionKind.modify: ext.EntryOperationKind.modify,
                        EntryActionKind.delete: ext.EntryOperationKind.delete,
                    }[entry_action.kind]
                )
                for entry_action in self._get_actions(clz, action_kind=None)
            ])

        return ext.ModificationPlan(
            operations=ret,
        )
