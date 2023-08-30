"""
Sample usage (replace connection and switch data source to subselect)::

    from bi_constants.enums import CreateDSFrom
    from bi_core.maintenance.helpers import get_sync_usm, get_dataset
    from bi_api_lib.maintenance.updates import SimpleDatasetUpdateGen, update_dataset

    subsql = 'SELECT * FROM my_table'
    ds = get_dataset('hfu4hg98wh48', is_async_env=False)
    us_manager = get_sync_usm()
    update_gen = SimpleDatasetUpdateGen(dataset=ds, us_manager=us_manager)
    update_dataset(
        dataset=ds,
        us_manager=us_manager,
        updates=[
            update_gen.replace_connection(old_id='abzgni02ra8a7', new_id='lg6crpowhm3ij'),
            update_gen.update_source_as_subselect(
                id='bb46b7c1-9d5f-11eb-841a-43a71976c9af',
                source_type=CreateDSFrom.CHYT_SUBSELECT, subsql=subsql,
            ),
        ],
    )

"""

import uuid
from typing import Optional, Sequence, List

import attr

from bi_api_lib.request_model.data import Action, FieldAction, AddUpdateSourceAction, ReplaceConnectionAction, \
    ReplaceConnection, SourceActionBase, AddField
from bi_constants.enums import BIType, CalcMode, AggregationFunction, CreateDSFrom, DataSourceRole

from bi_core.us_dataset import Dataset
from bi_core.us_manager.us_manager import USManagerBase
from bi_core.base_models import DefaultConnectionRef
from bi_core.fields import ParameterValueConstraint
from bi_core.values import BIValue
from bi_core.data_source.collection import DataSourceCollectionFactory
from bi_core.components.accessor import DatasetComponentAccessor

from bi_api_lib.enums import DatasetAction
from bi_api_lib.dataset.validator import DatasetValidator


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
            self, title: str,
            formula: Optional[str] = None,
            source: Optional[str] = None,
            cast: Optional[BIType] = None,
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
            'guid': field_id,
            'title': title,
            'source': source or '',
            'formula': formula or '',
            'calc_mode': calc_mode,
            'aggregation': aggregation,
            'default_value': default_value,
            'value_constraint': value_constraint,
        }
        if cast is not None:
            field_data['cast'] = cast

        action_data = {
            'action': DatasetAction.add_field,
            'field': AddField(**field_data),  # type: ignore
        }
        return FieldAction(**action_data)  # type: ignore

    def update_source_as_subselect(
            self,
            id: str,
            source_type: CreateDSFrom,
            subsql: str,
    ) -> AddUpdateSourceAction:
        assert source_type.name.endswith('_SUBSELECT'), 'Must be a *_SUBSELECT source type'

        dsrc_coll_spec = self._ds_accessor.get_data_source_coll_spec_strict(source_id=id)
        dsrc_coll_factory = DataSourceCollectionFactory(us_entry_buffer=self._us_manager.get_entry_buffer())
        dsrc_coll = dsrc_coll_factory.get_data_source_collection(spec=dsrc_coll_spec)

        dsrc = dsrc_coll.get_strict(role=DataSourceRole.origin)
        assert dsrc is not None
        conn_ref = dsrc.spec.connection_ref
        assert isinstance(conn_ref, DefaultConnectionRef), 'Origin connection must be a regular connection'

        source_data = {
            'id': id,
            'source_type': source_type,
            'connection_id': conn_ref.conn_id,
            'parameters': {
                'subsql': subsql,
            },
        }

        action_data = {
            'action': DatasetAction.update_source,
            'source': source_data,
        }
        return AddUpdateSourceAction(**action_data)  # type: ignore

    def replace_connection(self, old_id: str, new_id: str) -> ReplaceConnectionAction:
        connection_data = {'id': old_id, 'new_id': new_id}
        action_data = {
            'action': DatasetAction.replace_connection,
            'connection': ReplaceConnection(**connection_data),
        }
        return ReplaceConnectionAction(**action_data)  # type: ignore

    def refresh_source(self, source_id: str) -> SourceActionBase:
        action_data = {
            'action': DatasetAction.refresh_source,
            'source': {'id': source_id},
        }
        return SourceActionBase(**action_data)  # type: ignore

    def refresh_all_sources(self) -> List[SourceActionBase]:
        return [
            self.refresh_source(source_id)
            for source_id in self._ds_accessor.get_data_source_id_list()
        ]


def update_dataset(dataset: Dataset, updates: Sequence[Action], us_manager: USManagerBase) -> None:
    """
    Apply updates to the dataset.
    The dataset must be fetched in sync mode (get_dataset('...', is_async_env=False))
    """
    ds_validator = DatasetValidator(ds=dataset, us_manager=us_manager)
    ds_validator.apply_batch(action_batch=updates)
