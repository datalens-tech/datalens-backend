import logging
from typing import Optional, TypeVar, Type, Generic

import attr

from bi_external_api.domain.internal import (
    datasets,
    charts,
    dashboards,
)
from bi_external_api.domain.internal.dl_common import EntryInstance
from bi_external_api.internal_api_clients.charts_api import APIClientCharts
from bi_external_api.internal_api_clients.dash_api import APIClientDashboard
from bi_external_api.internal_api_clients.dataset_api import APIClientBIBackControlPlane
from bi_external_api.internal_api_clients.united_storage import MiniUSClient

LOGGER = logging.getLogger(__name__)

_INST_TYPE_TV = TypeVar("_INST_TYPE_TV", bound=EntryInstance)


@attr.s(frozen=True)
class InstanceLoadInfo(Generic[_INST_TYPE_TV]):
    requested_entry_id: str = attr.ib()
    may_be_instance: Optional[_INST_TYPE_TV] = attr.ib(default=None)
    may_be_exc: Optional[Exception] = attr.ib(default=None)

    def __attrs_post_init__(self) -> None:
        if self.may_be_instance is not None:
            assert self.may_be_exc is None
            assert self.may_be_instance.summary.id == self.requested_entry_id
        if self.may_be_exc is not None:
            assert self.may_be_instance is None

    def is_ok(self) -> bool:
        return self.may_be_instance is not None

    @property
    def instance(self) -> _INST_TYPE_TV:
        may_be_inst = self.may_be_instance
        assert may_be_inst is not None
        return may_be_inst

    @property
    def exc(self) -> Exception:
        may_be_exc = self.may_be_exc
        assert may_be_exc is not None
        return may_be_exc


@attr.s(frozen=True)
class InternalAPIClients:
    datasets_cp: APIClientBIBackControlPlane = attr.ib()
    charts: Optional[APIClientCharts] = attr.ib()
    dash: Optional[APIClientDashboard] = attr.ib()
    us: MiniUSClient = attr.ib()

    @property
    def charts_strict(self) -> APIClientCharts:
        cli = self.charts
        assert cli is not None, "Charts API client was not passed to InternalAPIClients"
        return cli

    @property
    def dash_strict(self) -> APIClientDashboard:
        cli = self.dash
        assert cli is not None, "Dashboards API client was not passed to InternalAPIClients"
        return cli

    @staticmethod
    def _wrap_to_load_info(inst: _INST_TYPE_TV, entry_id: str) -> InstanceLoadInfo[_INST_TYPE_TV]:
        return InstanceLoadInfo(may_be_instance=inst, requested_entry_id=entry_id)

    async def get_instance_by_id(self, clz: Type[_INST_TYPE_TV], entry_id: str) -> InstanceLoadInfo[_INST_TYPE_TV]:
        try:
            if clz is datasets.ConnectionInstance:
                # TODO FIX: Fix MyPy
                return self._wrap_to_load_info(
                    await self.datasets_cp.get_connection(entry_id),  # type: ignore
                    entry_id=entry_id,
                )  # type: ignore
            if clz is datasets.DatasetInstance:
                # TODO FIX: Fix MyPy
                return self._wrap_to_load_info(
                    await self.datasets_cp.get_dataset_instance(entry_id),  # type: ignore
                    entry_id=entry_id,
                )  # type: ignore
            if clz is charts.ChartInstance:
                # TODO FIX: Fix MyPy
                return self._wrap_to_load_info(
                    await self.charts_strict.get_chart(entry_id),  # type: ignore
                    entry_id=entry_id,
                )  # type: ignore
            if clz is dashboards.DashInstance:
                # TODO FIX: Fix MyPy
                return self._wrap_to_load_info(
                    await self.dash_strict.get_dashboard(entry_id),  # type: ignore
                    entry_id=entry_id,
                )  # type: ignore
        except Exception as err:  # TODO FIX: Minimize scope of stored exceptions
            LOGGER.error(f"Exception during entry loading: {clz}/{entry_id}", exc_info=True)
            return InstanceLoadInfo(may_be_exc=err, requested_entry_id=entry_id)
        raise AssertionError(f"Unexpected instance type: {clz}")
