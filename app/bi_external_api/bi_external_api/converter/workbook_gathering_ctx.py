from typing import (
    Iterable,
    Sequence,
    Type,
    TypeVar,
)

import attr

from bi_external_api.converter.id_gathering_processor import IDGatheringProcessor
from bi_external_api.converter.workbook import (
    EntryLoadFailInfo,
    WorkbookContext,
)
from bi_external_api.domain.internal import charts as chart_models
from bi_external_api.domain.internal import dashboards as dash_models
from bi_external_api.domain.internal import datasets as dataset_models
from bi_external_api.domain.internal.dl_common import (
    EntryInstance,
    EntrySummary,
)
from bi_external_api.domain.utils import ensure_tuple


_INST_TYPE_TV = TypeVar("_INST_TYPE_TV", bound=EntryInstance)
_NAME_NORMALIZER_TV = TypeVar("_NAME_NORMALIZER_TV", bound="NameNormalizer")


@attr.s()
class NameNormalizer:
    _wb_id: str = attr.ib()
    _instances: Sequence[EntryInstance] = attr.ib()
    _map_entry_id_load_exc: dict[Type[EntryInstance], dict[str, Exception]] = attr.ib()
    # Resulting section
    _renamed_instances: Sequence[EntryInstance] = attr.ib(init=False, factory=tuple)
    _renamed_lfi: Sequence[EntryLoadFailInfo] = attr.ib(init=False, factory=tuple)
    _map_new_name_to_original_summary: dict[str, EntrySummary] = attr.ib(init=False, factory=dict)

    def __attrs_post_init__(self) -> None:
        self._perform_rename()

    def _perform_rename(self) -> None:
        map_new_name_to_original_summary = {}
        renamed_instances: list[EntryInstance] = []
        renamed_lfi: list[EntryLoadFailInfo] = []

        for inst in self._instances:
            new_summary = self.normalize_instance_summary(type(inst), inst.summary)
            map_new_name_to_original_summary[new_summary.name] = inst.summary
            renamed_instances.append(attr.evolve(inst, summary=new_summary))

        for inst_type, map_entry_id_exc in self._map_entry_id_load_exc.items():
            for entry_id, exc in map_entry_id_exc.items():
                new_summary = self.normalize_load_fail_info_summary(inst_type, entry_id)
                map_new_name_to_original_summary[new_summary.name] = EntrySummary(
                    scope=inst_type.SCOPE,
                    id=entry_id,
                    workbook_id=self._wb_id,
                    # TODO FIX: Try to extract from exc/hint
                    name="--unresolved--",
                )
                renamed_lfi.append(
                    EntryLoadFailInfo(
                        summary=new_summary,
                        exception=exc,
                    )
                )

        self._renamed_instances = renamed_instances
        self._renamed_lfi = renamed_lfi
        self._map_new_name_to_original_summary = map_new_name_to_original_summary

    def normalize_instance_summary(self, the_type: Type[EntryInstance], summary: EntrySummary) -> EntrySummary:
        raise NotImplementedError()

    def normalize_load_fail_info_summary(self, the_type: Type[EntryInstance], entry_id: str) -> EntrySummary:
        raise NotImplementedError()

    def get_renamed_instances(self, the_type: Type[_INST_TYPE_TV]) -> Sequence[_INST_TYPE_TV]:
        return [inst for inst in self._renamed_instances if isinstance(inst, the_type)]

    def get_renamed_load_fail_info(self) -> Sequence[EntryLoadFailInfo]:
        return self._renamed_lfi

    def get_map_new_name_to_original_summary(self) -> dict[str, EntrySummary]:
        return self._map_new_name_to_original_summary

    @classmethod
    def create(
        cls: Type[_NAME_NORMALIZER_TV],
        wb_id: str,
        instances: Sequence[EntryInstance],
        map_entry_id_load_exc: dict[Type[EntryInstance], dict[str, Exception]],
    ) -> _NAME_NORMALIZER_TV:
        return cls(
            wb_id=wb_id,
            instances=instances,
            map_entry_id_load_exc=map_entry_id_load_exc,
        )


@attr.s()
class IDNameNormalizer(NameNormalizer):
    def normalize_instance_summary(self, the_type: Type[EntryInstance], summary: EntrySummary) -> EntrySummary:
        return attr.evolve(summary, workbook_id=self._wb_id, name=summary.id)

    def normalize_load_fail_info_summary(self, the_type: Type[EntryInstance], entry_id: str) -> EntrySummary:
        return EntrySummary(
            scope=the_type.SCOPE,
            id=entry_id,
            name=entry_id,
            workbook_id=self._wb_id,
        )


@attr.s()
class WorkbookGatheringContext:
    wb_id: str = attr.ib()
    initial_dash_ids_to_load: Sequence[str] = attr.ib(converter=ensure_tuple)  # type: ignore
    name_normalizer_cls: Type[NameNormalizer] = attr.ib()

    _id_gathering_processor: IDGatheringProcessor = attr.ib(init=False, factory=IDGatheringProcessor)
    _instances: list[EntryInstance] = attr.ib(init=False, factory=list)
    _map_entry_id_load_exc: dict[Type[EntryInstance], dict[str, Exception]] = attr.ib(init=False, factory=dict)
    _entry_load_fail_info_list: list[EntryLoadFailInfo] = attr.ib(init=False, factory=list)

    def get_ids_to_load(self, clz: Type[EntryInstance]) -> Iterable[str]:
        if clz is dash_models.DashInstance:
            return self.initial_dash_ids_to_load
        return self._id_gathering_processor.get_gathered_entry_ids(clz.SCOPE)

    def add_load_fail_info(self, clz: Type[EntryInstance], map_entry_id_to_load_exc: dict[str, Exception]) -> None:
        self._map_entry_id_load_exc.setdefault(clz, {}).update(map_entry_id_to_load_exc)

    def add_loaded_instances(self, inst_iterable: Iterable[EntryInstance]) -> None:
        for inst in inst_iterable:
            self.add_single_loaded_instance(inst)

    def add_single_loaded_instance(self, inst: EntryInstance) -> None:
        self._id_gathering_processor.process(inst)
        self._instances.append(inst)

    def get_loaded_instances(self, the_type: Type[_INST_TYPE_TV]) -> Sequence[_INST_TYPE_TV]:
        return [inst for inst in self._instances if isinstance(inst, the_type)]

    def build_workbook_context(self) -> tuple[WorkbookContext, dict[str, EntrySummary]]:
        nn = self._get_name_normalizer()

        return (
            WorkbookContext(
                connections=nn.get_renamed_instances(dataset_models.ConnectionInstance),
                datasets=nn.get_renamed_instances(dataset_models.DatasetInstance),
                charts=nn.get_renamed_instances(chart_models.ChartInstance),
                dashboards=nn.get_renamed_instances(dash_models.DashInstance),
                load_fail_info_collection=nn.get_renamed_load_fail_info(),
            ),
            nn.get_map_new_name_to_original_summary(),
        )

    def _get_name_normalizer(self) -> NameNormalizer:
        return self.name_normalizer_cls.create(
            wb_id=self.wb_id,
            instances=self._instances,
            map_entry_id_load_exc=self._map_entry_id_load_exc,
        )
