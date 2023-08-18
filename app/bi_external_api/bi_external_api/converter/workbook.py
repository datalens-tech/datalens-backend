from __future__ import annotations

import abc
from typing import FrozenSet, Optional, Iterable, Type, TypeVar, Sequence, ClassVar

import attr

from bi_external_api.domain.internal import (
    datasets as dataset_models,
    charts as chart_models,
    dashboards as dash_models,
)
from bi_external_api.domain.internal.dl_common import EntryInstance, EntrySummary
from . import converter_exc
from ..internal_api_clients.models import WorkbookBasicInfo

_ENTRY_CLS_TV = TypeVar("_ENTRY_CLS_TV", bound=EntryInstance)


@attr.s(frozen=True)
class EntryRef(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def match(self, summary: EntrySummary) -> bool:
        pass


@attr.s()
class EntryNameRef(EntryRef):
    name: str = attr.ib()

    def match(self, summary: EntrySummary) -> bool:
        return summary.name == self.name


@attr.s()
class EntryIdRef(EntryRef):
    id: str = attr.ib()

    def match(self, summary: EntrySummary) -> bool:
        return summary.id == self.id


@attr.s(frozen=True)
class EntryLoadFailInfo:
    summary: EntrySummary = attr.ib()
    exception: Exception = attr.ib(eq=False, hash=False, order=False)


# TODO FIX: MyPy fires next error in case of TypeVar
#  Type variable "bi_external_api.converter.workbook._ENTRY_CLS_TV" is unbound
def _collection_converter(raw_seq: Sequence[EntryInstance]) -> Sequence[EntryInstance]:
    return tuple(
        sorted((inst for inst in raw_seq), key=lambda inst: inst.summary.name)
    )


def _load_fail_info_collection_converter(raw_seq: Sequence[EntryLoadFailInfo]) -> Sequence[EntryLoadFailInfo]:
    return tuple(sorted(raw_seq, key=lambda lfi: (lfi.summary.scope.name, lfi.summary.name)))


@attr.s(frozen=True, auto_attribs=True)
class WorkbookContext:
    _SUPPORTED_INSTANCES: ClassVar[FrozenSet[Type[EntryInstance]]] = frozenset([
        dataset_models.DatasetInstance,
        chart_models.ChartInstance,
        dash_models.DashInstance,
    ])

    connections: Sequence[dataset_models.ConnectionInstance] = attr.ib(converter=_collection_converter)
    datasets: Sequence[dataset_models.DatasetInstance] = attr.ib(converter=_collection_converter)
    charts: Sequence[chart_models.ChartInstance] = attr.ib(converter=_collection_converter)
    dashboards: Sequence[dash_models.DashInstance] = attr.ib(converter=_collection_converter)
    load_fail_info_collection: Sequence[EntryLoadFailInfo] = attr.ib(
        converter=_load_fail_info_collection_converter,
        default=(),
    )
    wb_basic_info: Optional[WorkbookBasicInfo] = attr.ib(default=None)

    def __attrs_post_init__(self) -> None:
        ids_accumulator: set[str] = set()
        names_accumulator: set[str] = set()

        for clz in self._SUPPORTED_INSTANCES:
            attr_name, inst_lst = self._get_entry_coll_name_and_value(clz)
            for inst in inst_lst:
                assert type(inst) == clz, f"Got unexpected type of instance for {attr_name}: {type(inst)}"
                inst_name = inst.summary.name
                inst_id = inst.summary.id
                assert inst_name not in names_accumulator, f"Duplicated name in workbook context: {inst_name}"
                assert inst_id not in ids_accumulator, f"Duplicated ID in workbook context: {inst_id}"
                names_accumulator.add(inst_name)
                ids_accumulator.add(inst_id)

        for load_fail_info in self.load_fail_info_collection:
            summary = load_fail_info.summary
            assert summary.name not in names_accumulator, f"Duplicated name in workbook context: {summary.name}"
            assert summary.id not in ids_accumulator, f"Duplicated ID in workbook context: {summary.id}"
            names_accumulator.add(summary.name)
            ids_accumulator.add(summary.id)

    def _get_entry_coll_name_and_value(self, clz: Type[_ENTRY_CLS_TV]) -> tuple[str, Sequence[_ENTRY_CLS_TV]]:
        if clz is dataset_models.ConnectionInstance:
            return "connections", self.connections  # type: ignore
        if clz is dataset_models.DatasetInstance:
            return "datasets", self.datasets  # type: ignore
        if clz is chart_models.ChartInstance:
            return "charts", self.charts  # type: ignore
        if clz is dash_models.DashInstance:
            return "dashboards", self.dashboards  # type: ignore
        raise AssertionError()

    @classmethod
    def ref(cls, *, name: Optional[str] = None, id: Optional[str] = None) -> EntryRef:
        if name is not None and id is None:
            return EntryNameRef(name)
        if id is not None and name is None:
            return EntryIdRef(id)
        raise AssertionError()

    def get_all_summaries(self) -> Iterable[EntrySummary]:
        all_summaries: list[EntrySummary] = []
        for clz in self._SUPPORTED_INSTANCES:
            _, inst_seq = self._get_entry_coll_name_and_value(clz)
            all_summaries.extend(inst.summary for inst in inst_seq)

        all_summaries.extend(load_fail_info.summary for load_fail_info in self.load_fail_info_collection)

        return all_summaries

    def resolve_entry(
            self,
            clz: Type[_ENTRY_CLS_TV],
            ref: EntryRef,
    ) -> _ENTRY_CLS_TV:
        load_fail_info = self.resolve_entry_load_fail_info(ref)
        if load_fail_info:
            raise converter_exc.WorkbookEntryBroken(f"{clz.__name__}/{ref} is broken")

        _, entries_coll = self._get_entry_coll_name_and_value(clz)
        try:
            return next(entry for entry in entries_coll if ref.match(entry.summary))
        except StopIteration:
            raise converter_exc.WorkbookEntryNotFound(f"{clz.__name__}/{ref} not found in workbook")

    def resolve_entry_load_fail_info(self, ref: EntryRef) -> Optional[EntryLoadFailInfo]:
        return next(
            (lfi for lfi in self.load_fail_info_collection if ref.match(lfi.summary)),
            None
        )

    def resolve_summary(self, clz: Type[_ENTRY_CLS_TV], ref: EntryRef) -> EntrySummary:
        lfi = self.resolve_entry_load_fail_info(ref)
        if lfi is not None:
            assert clz.SCOPE == lfi.summary.scope, \
                f"resolve_summary() called for {clz}, but entry is {lfi.summary.scope}"
            return lfi.summary

        return self.resolve_entry(clz, ref).summary

    def resolve_connection_by_name(self, name: str) -> dataset_models.ConnectionInstance:
        return self.resolve_entry(dataset_models.ConnectionInstance, EntryNameRef(name))

    def resolve_dataset_by_name(self, name: str) -> dataset_models.DatasetInstance:
        return self.resolve_entry(dataset_models.DatasetInstance, EntryNameRef(name))

    def resolve_dataset_by_id(self, id: str) -> dataset_models.DatasetInstance:
        return self.resolve_entry(dataset_models.DatasetInstance, EntryIdRef(id))

    def resolve_chart_by_name(self, name: str) -> chart_models.ChartInstance:
        return self.resolve_entry(chart_models.ChartInstance, EntryNameRef(name))

    def resolve_chart_by_id(self, id: str) -> chart_models.ChartInstance:
        return self.resolve_entry(chart_models.ChartInstance, EntryIdRef(id))

    def resolve_dash_by_id(self, id: str) -> dash_models.DashInstance:
        return self.resolve_entry(dash_models.DashInstance, EntryIdRef(id))

    def resolve_dash_by_name(self, name: str) -> dash_models.DashInstance:
        return self.resolve_entry(dash_models.DashInstance, EntryNameRef(name))

    #
    # Mutators
    #
    def add_entries(self, entries: Iterable[EntryInstance]) -> WorkbookContext:
        separated_new_entries = self.create_from_instances_list(entries)
        updates: dict[str, Sequence[EntryInstance]] = {}

        for clz in self._SUPPORTED_INSTANCES:
            attr_name, inst_to_add_set = separated_new_entries._get_entry_coll_name_and_value(clz)
            if len(inst_to_add_set) > 0:
                _, current_inst_set = self._get_entry_coll_name_and_value(clz)
                updates[attr_name] = [*current_inst_set, *inst_to_add_set]

        return attr.evolve(self, **updates)

    def remove_entries(self, clz: Type[EntryInstance], refs: Iterable[EntryRef]) -> WorkbookContext:
        attr_name, current_entry_seq = self._get_entry_coll_name_and_value(clz)
        current_entry_list: list[EntryInstance] = list(current_entry_seq)
        current_load_fail_info_list = list(self.load_fail_info_collection)

        for ref in refs:
            try:
                idx = next(idx for idx, inst in enumerate(current_entry_list) if ref.match(inst.summary))
                current_entry_list.pop(idx)
            except StopIteration:
                try:
                    idx, summary = next(
                        (idx, load_fail.summary) for idx, load_fail in enumerate(current_load_fail_info_list)
                        if ref.match(load_fail.summary)
                    )
                    assert summary.scope == clz.SCOPE, \
                        f"Attempt to replace {clz}/{ref}, but referenced entry has scope {summary.scope}"
                    current_load_fail_info_list.pop(idx)
                except StopIteration:
                    raise AssertionError(f"Attempt to remove {clz}/{ref} that does not exist in workbook")

        return attr.evolve(
            self,
            load_fail_info_collection=current_load_fail_info_list,
            **{attr_name: current_entry_list},
        )

    def replace_entries(self, clz: Type[EntryInstance], entries: Iterable[EntryInstance]) -> WorkbookContext:
        attr_name, current_entry_seq = self._get_entry_coll_name_and_value(clz)
        current_entry_list: list[EntryInstance] = list(current_entry_seq)
        current_load_fail_info_list = list(self.load_fail_info_collection)

        for new_inst in entries:
            ref = self.ref(id=new_inst.summary.id)

            try:
                idx, inst = next((idx, inst) for idx, inst in enumerate(current_entry_list) if ref.match(inst.summary))
                assert inst.summary == new_inst.summary
                current_entry_list[idx] = new_inst
            except StopIteration:
                idx, broken_summary = next(
                    (idx, load_fail_info.summary) for idx, load_fail_info in enumerate(current_load_fail_info_list)
                    if ref.match(load_fail_info.summary)
                )
                assert new_inst.summary == broken_summary
                current_load_fail_info_list.pop(idx)
                current_entry_list.append(new_inst)

        return attr.evolve(self, load_fail_info_collection=current_load_fail_info_list,
                           **{attr_name: current_entry_list})

    def replace_id(self, clz: Type[EntryInstance], old: str, new: str) -> WorkbookContext:
        attr_name, current_entry_seq = self._get_entry_coll_name_and_value(clz)
        current_entry_list: list[EntryInstance] = list(current_entry_seq)
        ref = self.ref(id=old)
        idx, inst = next((idx, inst) for idx, inst in enumerate(current_entry_list) if ref.match(inst.summary))
        modified_inst = attr.evolve(
            inst,
            summary=attr.evolve(
                inst.summary,
                id=new
            )
        )
        current_entry_list[idx] = modified_inst
        return attr.evolve(self, **{attr_name: current_entry_list})

    def taint_existing_entries(
            self,
            clz: Type[EntryInstance],
            taint_info_seq: Sequence[tuple[EntryRef, Exception]],
    ) -> WorkbookContext:
        return self.remove_entries(
            clz,
            [ref for ref, _ in taint_info_seq],
        ).add_tainted_entries([
            (self.resolve_summary(clz, ref), exc)
            for ref, exc in taint_info_seq
        ])

    def add_tainted_entries(self, taint_info_seq: Iterable[tuple[EntrySummary, Exception]]) -> WorkbookContext:
        return attr.evolve(self, load_fail_info_collection=[
            *self.load_fail_info_collection,
            *[EntryLoadFailInfo(summary=summary, exception=exc) for summary, exc in taint_info_seq],
        ])

    #
    # Constructors
    #
    @classmethod
    def create_from_instances_list(cls, inst_iterable: Iterable[EntryInstance]) -> WorkbookContext:
        ds_inst_lst: list[dataset_models.DatasetInstance] = []
        conn_inst_lst: list[dataset_models.ConnectionInstance] = []
        chart_inst_lst: list[chart_models.ChartInstance] = []
        dash_inst_lst: list[dash_models.DashInstance] = []
        rest_lst: list[EntryInstance] = []

        for ints in inst_iterable:
            if isinstance(ints, dataset_models.DatasetInstance):
                ds_inst_lst.append(ints)
            elif isinstance(ints, dataset_models.ConnectionInstance):
                conn_inst_lst.append(ints)
            elif isinstance(ints, chart_models.ChartInstance):
                chart_inst_lst.append(ints)
            elif isinstance(ints, dash_models.DashInstance):
                dash_inst_lst.append(ints)
            else:
                rest_lst.append(ints)

        assert len(rest_lst) == 0
        return cls(
            datasets=ds_inst_lst,
            charts=chart_inst_lst,
            dashboards=dash_inst_lst,
            connections=conn_inst_lst,
        )
