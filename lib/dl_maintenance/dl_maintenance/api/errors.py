"""
Sample usage (remove phantom errors from dataset):

from dl_api_lib.app_settings import DataApiAppSettings
from dl_data_api.app_factory import StandaloneDataApiAppFactory

from dl_maintenance.api.common import MaintenanceEnvironmentManager
from dl_maintenance.api.helpers import get_dataset
from dl_maintenance.api.errors import ComponentErrorManager

mm = MaintenanceEnvironmentManager(app_settings_cls=DataApiAppSettings, app_factory_cls=StandaloneDataApiAppFactory)
us_manager = mm.get_usm_from_env()
ds = get_dataset(mm, 'hfu4hg98wh48', is_async_env=False)
error_mgr = ComponentErrorManager(dataset=ds, us_manager=us_manager)
phantom_refs = error_mgr.find_phantom_error_refs()
error_mgr.print_component_refs(phantom_refs)
# error_mgr.remove_errors_for_refs(phantom_refs)
# us_manager.save(ds)

"""

from collections import defaultdict
from typing import (
    Any,
    Dict,
    List,
    Optional,
)
import uuid

import attr

from dl_api_lib.dataset.component_abstraction import (
    DatasetComponentAbstraction,
    DatasetComponentRef,
)
from dl_constants.enums import ComponentType
from dl_core.us_dataset import Dataset
from dl_core.us_manager.us_manager import USManagerBase


TACTION = Dict[str, Any]


def _make_component_id() -> str:
    return str(uuid.uuid4())


@attr.s
class ComponentErrorManager:
    _dataset: Optional[Dataset] = attr.ib(kw_only=True, default=None)
    _dca: DatasetComponentAbstraction = attr.ib(init=False)
    _us_manager: USManagerBase = attr.ib(kw_only=True)

    def __attrs_post_init__(self) -> None:
        self._dca = DatasetComponentAbstraction(
            dataset=self.dataset, us_entry_buffer=self._us_manager.get_entry_buffer()
        )

    @property
    def dataset(self) -> Dataset:
        assert self._dataset is not None
        return self._dataset

    def find_phantom_error_refs(self) -> List[DatasetComponentRef]:
        all_error_refs = [
            DatasetComponentRef(component_type=item.type, component_id=item.id)
            for item in self.dataset.error_registry.items
        ]
        phantom_refs: List[DatasetComponentRef] = []
        for component_ref in all_error_refs:
            if self._dca.get_component(component_ref=component_ref) is None:
                phantom_refs.append(component_ref)

        return phantom_refs

    @staticmethod
    def print_component_refs(component_refs: List[DatasetComponentRef]) -> None:
        by_comp_type: Dict[ComponentType, List[str]] = defaultdict(list)
        for component_ref in component_refs:
            by_comp_type[component_ref.component_type].append(component_ref.component_id)

        for component_type, component_id_list in sorted(by_comp_type.items(), key=lambda el: el[0].name):
            print(f"{component_type.name}:")
            for component_id in sorted(component_id_list):
                print(f"    {component_id}")

    def remove_errors_for_refs(self, component_refs: List[DatasetComponentRef]) -> None:
        for component_ref in component_refs:
            self.dataset.error_registry.remove_errors(id=component_ref.component_id)
