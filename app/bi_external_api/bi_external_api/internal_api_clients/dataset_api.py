import logging
from typing import (
    Any,
    NoReturn,
    Optional,
    Sequence,
    Tuple,
)

import attr

from bi_external_api.domain.internal import datasets
from bi_external_api.domain.internal.dl_common import (
    EntryScope,
    EntrySummary,
)
from bi_external_api.domain.internal.mapper import internal_models_mapper
from dl_api_commons.client.base import (
    Req,
    Resp,
)
from dl_api_commons.client.common import CommonInternalAPIClient
from dl_constants.enums import ConnectionType

from . import exc_api
from .constants import DatasetOpCode
from .models import WorkbookBackendTOC


LOGGER = logging.getLogger(__name__)


@attr.s()
class APIClientBIBackControlPlane(CommonInternalAPIClient):
    def get_naming_args(self, wb_id: str, name: str) -> dict[str, str]:
        if self._use_workbooks_api:
            return dict(
                name=name,
                workbook_id=wb_id,
            )
        else:
            return dict(
                dir_path=wb_id,
                name=name,
            )

    async def create_connection(
        self,
        *,
        wb_id: str,
        name: str,
        conn_data: dict[str, Any],
    ) -> datasets.ConnectionInstance:
        op_code = DatasetOpCode.CONNECTION_CREATE

        req = Req(
            method="POST",
            url="/api/v1/connections",
            data_json={
                **conn_data,
                **self.get_naming_args(wb_id=wb_id, name=name),
            },
            require_ok=False,
        )
        resp = await self.make_request(req)
        if resp.status != 200:
            self.raise_from_resp(resp, op_code=op_code)

        conn_id = resp.json["id"]
        return await self.get_connection(conn_id)

    async def modify_connection(
        self,
        *,
        wb_id: str,
        name: str,
        conn_id: str,
        conn_data: dict[str, Any],
    ) -> datasets.ConnectionInstance:
        op_code = DatasetOpCode.CONNECTION_MODIFY

        req = Req(
            method="PUT",
            url=f"/api/v1/connections/{conn_id}",
            data_json={
                **conn_data,
                "name": name,
            },
            require_ok=False,
        )
        resp = await self.make_request(req)
        if resp.status != 200:
            self.raise_from_resp(resp, op_code=op_code)

        return await self.get_connection(conn_id)

    async def delete_connection(self, conn_id: str) -> None:
        op_code = DatasetOpCode.CONNECTION_DELETE

        resp = await self.make_request(Req(method="DELETE", url=f"/api/v1/connections/{conn_id}", require_ok=False))
        if resp.status != 200:
            self.raise_from_resp(resp, op_code=op_code)

    async def _get_connection(self, conn_id: str) -> Tuple[datasets.ConnectionInstance, Resp]:
        op_code = DatasetOpCode.CONNECTION_GET
        req = Req(
            method="GET",
            url=f"/api/v1/connections/{conn_id}",
            require_ok=False,
        )
        resp = await self.make_request(req)
        if resp.status != 200:
            self.raise_from_resp(resp, op_code=op_code)
        true_workbooks = self._use_workbooks_api
        summary: EntrySummary = EntrySummary.from_key(
            scope=EntryScope.connection,
            entry_id=conn_id,
            key=resp.json["key"],
            true_workbook=true_workbooks,
            workbook_id=resp.json["workbook_id"] if true_workbooks else None,
        )

        conn_inst = datasets.ConnectionInstance(
            summary=summary,
            type=ConnectionType[resp.json["db_type"]],
        )
        assert conn_inst.summary.id == conn_id
        return conn_inst, resp

    async def get_connection(self, conn_id: str) -> datasets.ConnectionInstance:
        conn_inst, _ = await self._get_connection(conn_id)
        return conn_inst

    async def get_connection_with_data(self, conn_id: str) -> tuple[datasets.ConnectionInstance, dict[str, Any]]:
        conn_inst, resp = await self._get_connection(conn_id)
        return conn_inst, resp.json

    @staticmethod
    def load_entries_summary(
        wb_id: str, entry_scope: EntryScope, js: dict[str, dict[str, Any]]
    ) -> frozenset[EntrySummary]:
        return frozenset(
            EntrySummary(
                workbook_id=wb_id,
                name=entry_name,
                id=entry_props["id"],
                scope=entry_scope,
            )
            for entry_name, entry_props in js.items()
        )

    def _handle_wb_404(self, resp: Resp, op_code: str) -> NoReturn:
        data = None
        try:
            data = resp.json
        except Exception:  # noqa
            self.raise_from_resp(resp, op_code=op_code)

        if not isinstance(data, dict) or data.get("code") != "ERR.DS_API.US.OBJ_NOT_FOUND":
            self.raise_from_resp(resp, op_code=op_code)

        common_data = self.create_exc_data(resp, op_code)
        raise exc_api.WorkbookNotFound(common_data)

    async def get_workbook_backend_toc(self, workbook_id: str) -> WorkbookBackendTOC:
        op_code = DatasetOpCode.WORKBOOK_INFO_GET

        req = Req(
            method="GET",
            url=f"/api/v1/info/internal/pseudo_workbook/{workbook_id}",
            require_ok=False,
        )
        resp = await self.make_request(req)

        if resp.status == 404:
            self._handle_wb_404(resp, op_code)
        elif resp.status != 200:
            self.raise_from_resp(resp, op_code=op_code)

        conn_map = resp.json["connections"]
        dataset_map = resp.json["datasets"]
        charts_map = resp.json.get("charts", {})
        dash_map = resp.json.get("dashboards", {})

        return WorkbookBackendTOC(
            connections=frozenset(
                datasets.BIConnectionSummary(
                    common_summary=EntrySummary(
                        scope=EntryScope.connection,
                        name=conn_name,
                        id=conn_props["id"],
                        workbook_id=workbook_id,
                    ),
                    type=ConnectionType[conn_props["type"]],
                )
                for conn_name, conn_props in conn_map.items()
            ),
            datasets=self.load_entries_summary(js=dataset_map, wb_id=workbook_id, entry_scope=EntryScope.dataset),
            charts=self.load_entries_summary(js=charts_map, wb_id=workbook_id, entry_scope=EntryScope.widget),
            dashboards=self.load_entries_summary(js=dash_map, wb_id=workbook_id, entry_scope=EntryScope.dash),
        )

    # TODO FIX: Rename
    async def build_dataset_config_by_actions(
        self,
        actions: Sequence[datasets.Action],
        dataset_id: Optional[str] = None,
    ) -> tuple[datasets.Dataset, dict[str, Any]]:
        op_code = DatasetOpCode.DATASET_VALIDATE

        action_schema = internal_models_mapper.get_or_create_schema_for_attrs_class(datasets.Action)()
        dataset_schema = internal_models_mapper.get_or_create_schema_for_attrs_class(datasets.Dataset)()

        data = dict(updates=action_schema.dump(actions, many=True))
        url: str

        if dataset_id is None:
            url = "/api/v1/datasets/validators/dataset"
        else:
            url = f"/api/v1/datasets/{dataset_id}/versions/draft/validators/schema"

        req = Req(
            method="POST",
            url=url,
            data_json=data,
            require_ok=False,
        )
        resp = await self.make_request(req)

        if resp.status == 200:
            with self.deserialization_err_handler(resp, op_code=op_code):
                dataset = dataset_schema.load(resp.json["dataset"])
                return dataset, resp.json

        if resp.status == 400:
            err_code = resp.json.get("code")

            if err_code == "ERR.DS_API.VALIDATION.ERROR":
                try:
                    with self.deserialization_err_handler(resp, op_code=op_code):
                        dataset = dataset_schema.load(resp.json["dataset"])
                    raise exc_api.DatasetValidationError(
                        exc_api.DatasetValidationErrorData(
                            message="Dataset validation fail",
                            dataset=dataset,
                        )
                    )
                except exc_api.DatasetValidationError:
                    raise
                except Exception:
                    LOGGER.exception("Exception during preparing exception for 'ERR.DS_API.VALIDATION.ERROR'")
                    raise exc_api.DatasetValidationError(
                        exc_api.DatasetValidationErrorData(
                            message="Dataset validation fail. Errors are unavailable.",
                            dataset=None,
                        )
                    )

        self.raise_from_resp(resp, op_code=op_code)

    async def get_dataset(self, id: str) -> datasets.Dataset:
        return (await self.get_dataset_instance(id)).dataset

    async def get_dataset_instance(self, id: str) -> datasets.DatasetInstance:
        op_code = DatasetOpCode.DATASET_GET

        dataset_schema = internal_models_mapper.get_or_create_schema_for_attrs_class(datasets.Dataset)()

        req = Req(
            method="GET",
            url=f"/api/v1/datasets/{id}/versions/draft",
            require_ok=False,
        )
        resp = await self.make_request(req)

        if resp.status != 200:
            self.raise_from_resp(resp, op_code=op_code)

        with self.deserialization_err_handler(resp, op_code=op_code):
            dataset = dataset_schema.load(resp.json["dataset"])

        true_workbooks = self._use_workbooks_api

        summary: EntrySummary = EntrySummary.from_key(
            scope=EntryScope.dataset,
            entry_id=id,
            key=resp.json["key"],
            true_workbook=true_workbooks,
            workbook_id=resp.json["workbook_id"] if true_workbooks else None,
        )

        return datasets.DatasetInstance(
            summary=summary,
            dataset=dataset,
            backend_local_revision_id=resp.json["dataset"]["revision_id"],
        )

    # TODO FIX: Try to use Dataset class when all fields will be handled
    async def create_dataset(self, validation_resp: dict, *, workbook_id: str, name: str) -> EntrySummary:
        op_code = DatasetOpCode.DATASET_CREATE

        data = dict(
            dataset=validation_resp["dataset"],
            **self.get_naming_args(wb_id=workbook_id, name=name),
        )

        req = Req(
            method="POST",
            url="/api/v1/datasets",
            data_json=data,
            require_ok=False,
        )
        resp = await self.make_request(req)

        if resp.status != 200:
            self.raise_from_resp(resp, op_code=op_code)

        return EntrySummary(
            id=resp.json["id"],
            name=name,
            workbook_id=workbook_id,
            scope=EntryScope.dataset,
        )

    # TODO FIX: Try to use Dataset class when all fields will be handled
    async def modify_dataset(
        self,
        validation_resp: dict,
        *,
        ds_id: str,
        backend_local_revision_id: Optional[str] = None,
    ) -> None:
        op_code = DatasetOpCode.DATASET_MODIFY

        dataset_cfg = validation_resp["dataset"]
        if backend_local_revision_id is not None:
            dataset_cfg = {
                **dataset_cfg,
                "revision_id": backend_local_revision_id,
            }

        data = dict(
            dataset=dataset_cfg,
        )

        req = Req(
            method="PUT",
            url=f"/api/v1/datasets/{ds_id}/versions/draft",
            data_json=data,
            require_ok=False,
        )
        resp = await self.make_request(req)
        if resp.status != 200:
            self.raise_from_resp(resp, op_code=op_code)

    async def delete_dataset(self, ds_id: str) -> None:
        op_code = DatasetOpCode.DATASET_REMOVE

        resp = await self.make_request(Req(method="DELETE", url=f"/api/v1/datasets/{ds_id}", require_ok=False))
        if resp.status != 200:
            self.raise_from_resp(resp, op_code=op_code)
