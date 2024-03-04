"""
Sample usage (replace connection and switch data source to subselect)::

from dl_api_lib.app_settings import DataApiAppSettings
from dl_data_api.app_factory import StandaloneDataApiAppFactory

from dl_maintenance.api.common import MaintenanceEnvironmentManager
from dl_maintenance.api.helpers import get_dataset
from dl_maintenance.api.updates import SimpleDatasetUpdateGen, update_dataset
from dl_connector_chyt.core.constants import SOURCE_TYPE_CHYT_YTSAURUS_SUBSELECT

mm = MaintenanceEnvironmentManager(app_settings_cls=DataApiAppSettings, app_factory_cls=StandaloneDataApiAppFactory)
subsql = "SELECT * FROM my_table"
ds = get_dataset(mm, "hfu4hg98wh48", is_async_env=False)
us_manager = mm.get_usm_from_env()
update_gen = SimpleDatasetUpdateGen(dataset=ds, us_manager=us_manager)
update_dataset(
    dataset=ds,
    us_manager=us_manager,
    updates=[
        update_gen.replace_connection(old_id="abzgni02ra8a7", new_id="lg6crpowhm3ij"),
        update_gen.update_source_as_subselect(
            id="bb46b7c1-9d5f-11eb-841a-43a71976c9af",
            source_type=SOURCE_TYPE_CHYT_YTSAURUS_SUBSELECT, subsql=subsql,
        ),
    ],
)

"""

from typing import (
    List,
    Optional,
    Sequence,
)
import uuid

import attr

from dl_api_lib.dataset.validator import DatasetValidator
from dl_api_lib.enums import DatasetAction
from dl_api_lib.request_model.data import (
    Action,
    AddField,
    AddUpdateSourceAction,
    FieldAction,
    ReplaceConnection,
    ReplaceConnectionAction,
    SourceActionBase,
)
from dl_constants.enums import (
    AggregationFunction,
    CalcMode,
    DataSourceRole,
    DataSourceType,
    UserDataType,
)
from dl_core.base_models import DefaultConnectionRef
from dl_core.components.accessor import DatasetComponentAccessor
from dl_core.data_source.collection import DataSourceCollectionFactory
from dl_core.fields import ParameterValueConstraint
from dl_core.us_dataset import Dataset
from dl_core.us_manager.us_manager import USManagerBase
from dl_model_tools.typed_values import BIValue


def _make_component_id() -> str:
    return str(uuid.uuid4())


@attr.s
class SimpleDatasetUpdateGen:
    _dataset: Dataset = attr.ib(kw_only=True)
    _us_manager: USManagerBase = attr.ib(kw_only=True)
    _ds_accessor: DatasetComponentAccessor = attr.ib(init=False)

    @_ds_accessor.default
    def _make_ds_accessor(self) -> DatasetComponentAccessor:
        return DatasetComponentAccessor(dataset=self._dataset)

    @property
    def dataset(self) -> Dataset:
        return self._dataset

    def add_field(
        self,
        title: str,
        formula: Optional[str] = None,
        source: Optional[str] = None,
        cast: Optional[UserDataType] = None,
        aggregation: AggregationFunction = AggregationFunction.none,
        default_value: Optional[BIValue] = None,
        value_constraint: Optional[ParameterValueConstraint] = None,
    ) -> FieldAction:
        if default_value is not None:
            calc_mode = CalcMode.parameter
        elif formula:
            calc_mode = CalcMode.formula
        else:
            calc_mode = CalcMode.direct

        field_id = _make_component_id()
        title = title or field_id

        field_data = {
            "guid": field_id,
            "title": title,
            "source": source or "",
            "formula": formula or "",
            "calc_mode": calc_mode,
            "aggregation": aggregation,
            "default_value": default_value,
            "value_constraint": value_constraint,
        }
        if cast is not None:
            field_data["cast"] = cast

        action_data = {
            "action": DatasetAction.add_field,
            "field": AddField(**field_data),  # type: ignore  # 2024-01-30 # TODO: Argument 1 to "AddField" has incompatible type "**dict[str, object]"; expected "str | None"  [arg-type]
        }
        return FieldAction(**action_data)  # type: ignore  # 2024-01-30 # TODO: Argument 1 to "FieldAction" has incompatible type "**dict[str, object]"; expected "DatasetAction"  [arg-type]

    def update_source_as_subselect(
        self,
        id: str,
        source_type: DataSourceType,
        subsql: str,
    ) -> AddUpdateSourceAction:
        assert source_type.name.endswith("_SUBSELECT"), "Must be a *_SUBSELECT source type"

        dsrc_coll_spec = self._ds_accessor.get_data_source_coll_spec_strict(source_id=id)
        dsrc_coll_factory = DataSourceCollectionFactory(us_entry_buffer=self._us_manager.get_entry_buffer())
        dsrc_coll = dsrc_coll_factory.get_data_source_collection(spec=dsrc_coll_spec)

        dsrc = dsrc_coll.get_strict(role=DataSourceRole.origin)
        assert dsrc is not None
        conn_ref = dsrc.spec.connection_ref
        assert isinstance(conn_ref, DefaultConnectionRef), "Origin connection must be a regular connection"

        source_data = {
            "id": id,
            "source_type": source_type,
            "connection_id": conn_ref.conn_id,
            "parameters": {
                "subsql": subsql,
            },
        }

        action_data = {
            "action": DatasetAction.update_source,
            "source": source_data,
        }
        return AddUpdateSourceAction(**action_data)  # type: ignore  # 2024-01-30 # TODO: Argument 1 to "AddUpdateSourceAction" has incompatible type "**dict[str, object]"; expected "DatasetAction"  [arg-type]

    def replace_connection(self, old_id: str, new_id: str) -> ReplaceConnectionAction:
        connection_data = {"id": old_id, "new_id": new_id}
        action_data = {
            "action": DatasetAction.replace_connection,
            "connection": ReplaceConnection(**connection_data),
        }
        return ReplaceConnectionAction(**action_data)  # type: ignore  # 2024-01-30 # TODO: Argument 1 to "ReplaceConnectionAction" has incompatible type "**dict[str, object]"; expected "DatasetAction"  [arg-type]

    def refresh_source(self, source_id: str) -> SourceActionBase:
        action_data = {
            "action": DatasetAction.refresh_source,
            "source": {"id": source_id},
        }
        return SourceActionBase(**action_data)  # type: ignore  # 2024-01-30 # TODO: Argument 1 to "SourceActionBase" has incompatible type "**dict[str, object]"; expected "DatasetAction"  [arg-type]

    def refresh_all_sources(self) -> List[SourceActionBase]:
        return [self.refresh_source(source_id) for source_id in self._ds_accessor.get_data_source_id_list()]


def update_dataset(dataset: Dataset, updates: Sequence[Action], us_manager: USManagerBase) -> None:
    """
    Apply updates to the dataset.
    The dataset must be fetched in sync mode (get_dataset('...', is_async_env=False))
    """
    ds_validator = DatasetValidator(ds=dataset, us_manager=us_manager)
    ds_validator.apply_batch(action_batch=updates)
