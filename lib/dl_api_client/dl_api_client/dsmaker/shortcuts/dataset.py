from http import HTTPStatus
import typing
from typing import (
    Any,
    Callable,
    Dict,
    Optional,
)

from dl_api_client.dsmaker.api.dataset_api import SyncHttpDatasetApiV1
from dl_api_client.dsmaker.primitives import (
    BaseParameterValueConstraint,
    Dataset,
    ParameterValue,
)


def _add_anything_to_dataset(
    *,
    control_api: SyncHttpDatasetApiV1,
    dataset: Optional[Dataset] = None,
    dataset_id: Optional[str] = None,
    updater: Callable[[Dataset], Dataset],
    exp_status: int = HTTPStatus.OK,
    save: bool = True,
) -> Dataset:
    if dataset is None:
        assert dataset_id is not None
        ds = control_api.load_dataset(dataset=Dataset(id=dataset_id)).dataset
    else:
        ds = dataset

    ds = updater(ds)

    ds_resp = control_api.apply_updates(dataset=ds, fail_ok=True)
    assert ds_resp.status_code == exp_status, ds_resp.response_errors
    ds = ds_resp.dataset

    if save:
        ds = control_api.save_dataset(ds).dataset

    return ds


def add_formulas_to_dataset(
    *,
    api_v1: SyncHttpDatasetApiV1,  # FIXME: Rename to control_api
    dataset: Optional[Dataset] = None,
    dataset_id: Optional[str] = None,
    formulas: Dict[str, str],
    exp_status: int = HTTPStatus.OK,
    save: bool = True,
) -> Dataset:
    def _add_formulas(ds: Dataset) -> Dataset:
        for field_name, formula in formulas.items():
            ds.result_schema[field_name] = ds.field(formula=formula)
        return ds

    return _add_anything_to_dataset(
        control_api=api_v1,
        dataset=dataset,
        dataset_id=dataset_id,
        updater=_add_formulas,
        exp_status=exp_status,
        save=save,
    )


class Parameter(typing.NamedTuple):
    default_value: ParameterValue
    constraint: Optional[BaseParameterValueConstraint]
    template_enabled: bool


def add_parameters_to_dataset(
    *,
    api_v1: SyncHttpDatasetApiV1,  # FIXME: Rename to control_api
    dataset: Optional[Dataset] = None,
    dataset_id: Optional[str] = None,
    parameters: Dict[str, Parameter],
    exp_status: int = HTTPStatus.OK,
    save: bool = True,
) -> Dataset:
    def _add_parameters(ds: Dataset) -> Dataset:
        for field_name, parameter in parameters.items():
            ds.result_schema[field_name] = ds.field(
                cast=parameter.default_value.type,
                default_value=parameter.default_value,
                value_constraint=parameter.constraint,
                template_enabled=parameter.template_enabled,
            )
        return ds

    return _add_anything_to_dataset(
        control_api=api_v1,
        dataset=dataset,
        dataset_id=dataset_id,
        updater=_add_parameters,
        exp_status=exp_status,
        save=save,
    )


def create_basic_dataset(
    *,
    api_v1: SyncHttpDatasetApiV1,  # FIXME: Rename to control_api
    connection_id: str,
    data_source_settings: Dict[str, Any],
    formulas: Optional[Dict[str, str]] = None,
    save: bool = True,
) -> Dataset:
    ds = Dataset()
    ds.sources["source_1"] = ds.source(connection_id=connection_id, **data_source_settings)
    ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()
    ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.OK, ds_resp.response_errors
    ds = ds_resp.dataset

    if formulas is not None:
        ds = add_formulas_to_dataset(api_v1=api_v1, dataset=ds, formulas=formulas, save=False)

    if save:
        ds = api_v1.save_dataset(ds).dataset

    return ds
