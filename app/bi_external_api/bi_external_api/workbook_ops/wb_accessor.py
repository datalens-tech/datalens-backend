from typing import TypeVar, Type, Iterable, ClassVar

import attr

from bi_external_api.domain import external as ext

_ENTRY_INSTANCE_TV = TypeVar("_ENTRY_INSTANCE_TV", bound=ext.EntryInstance)


@attr.s(frozen=True)
class WorkbookAccessor:
    wb: ext.WorkBook = attr.ib()

    _SUPPORTED_INSTANCE_CLZ: ClassVar[frozenset[Type[ext.EntryInstance]]] = frozenset([
        ext.DatasetInstance,
        ext.ChartInstance,
        ext.DashInstance,
    ])

    def get_actual_instances(self, entry_clz: Type[_ENTRY_INSTANCE_TV]) -> Iterable[_ENTRY_INSTANCE_TV]:
        if entry_clz is ext.DatasetInstance:
            return self.wb.datasets  # type: ignore
        if entry_clz is ext.ChartInstance:
            return self.wb.charts  # type: ignore
        if entry_clz is ext.DashInstance:
            return self.wb.dashboards  # type: ignore

        raise AssertionError(f"Unknown type of external entry instance {entry_clz=}")

    @property
    def all_names(self) -> frozenset[str]:
        names_accumulator: set[str] = set()

        for clz in self._SUPPORTED_INSTANCE_CLZ:
            clz_name_list = [inst.name for inst in self.get_actual_instances(clz)]
            clz_name_set = set(clz_name_list)
            assert len(clz_name_list) == len(clz_name_set), f"Duplicated names in workbook instances {clz}"
            clz_name_intersection = names_accumulator & clz_name_set
            assert not clz_name_intersection, f"Duplicated names in workbook {clz_name_intersection}"

        return frozenset(names_accumulator)
