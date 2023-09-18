import re
from typing import (
    Any,
    Optional,
)

import attr

from bi_external_api.domain.internal.datasets import BIConnectionSummary
from bi_external_api.domain.internal.dl_common import (
    EntryScope,
    EntrySummary,
)
from dl_api_commons.base_models import AuthData
from dl_api_commons.client.base import Req
from dl_api_commons.client.common import CommonInternalAPIClient
from dl_constants.api_constants import (
    DLCookies,
    DLHeaders,
    DLHeadersCommon,
)
from dl_constants.enums import ConnectionType
from dl_us_client.constants import OpCode as UsOpCode
from dl_us_client.us_workbook_cmd_client import USWorkbookCommandClient

from .base import CollectionPagedContentsProvider
from .exc_api import InvalidIDFormatError
from .models import (
    CollectionContentsPage,
    CollectionInfo,
    WorkbookBackendTOC,
    WorkbookBasicInfo,
    WorkbookInCollectionInfo,
)


@attr.s()
class USMasterAuthData(AuthData):
    us_master_token: str = attr.ib(repr=False)

    def get_headers(self) -> dict[DLHeaders, str]:
        return {DLHeadersCommon.US_MASTER_TOKEN: self.us_master_token}

    def get_cookies(self) -> dict[DLCookies, str]:
        return {}


@attr.s()
class MiniUSClient(CommonInternalAPIClient, CollectionPagedContentsProvider):
    ID_RE = re.compile(r"^[0-9a-z]{13}$")

    _us_workbook_cmd_client: USWorkbookCommandClient = attr.ib(default=None)

    def __attrs_post_init__(self) -> None:
        self._us_workbook_cmd_client = USWorkbookCommandClient(
            base_url=self._base_url,
            tenant=self._tenant,
            auth_data=self._auth_data,
            req_id=self._req_id,
            extra_headers=self._extra_headers,
            session=self._session,
            use_workbooks_api=self._use_workbooks_api,
            read_only=self._read_only,
        )

    def validate_id(self, id: str) -> str:
        if self.ID_RE.match(id):
            return id
        raise InvalidIDFormatError()

    def _ensure_workbook_use_workbooks_api(self, use_workbooks_api: bool) -> None:
        assert use_workbooks_api == self._use_workbooks_api

    async def create_folder(self, key: str) -> None:
        self._ensure_workbook_use_workbooks_api(False)
        op_code = UsOpCode.WB_CREATE

        req = Req(
            method="post",
            url="v1/entries",
            data_json=dict(
                scope="folder",
                key=key,
                type="",
                recursion=True,
                hidden=False,
                meta={},
            ),
            require_ok=False,
        )
        resp = await self.make_request(req)

        if resp.status == 200:
            return None

        self.raise_from_resp(resp, op_code=op_code)

    async def is_folder_exists(self, key: str) -> bool:
        self._ensure_workbook_use_workbooks_api(False)
        op_code = UsOpCode.WB_CHECK_EXISTS

        req = Req(
            method="get",
            url="v1/entriesByKey",
            params={"key": key},
            require_ok=False,
        )
        resp = await self.make_request(req)

        if resp.status == 200:
            return True
        if resp.status == 404:
            return False

        self.raise_from_resp(resp, op_code=op_code)

    async def get_folder_content(self, us_path: str) -> frozenset[EntrySummary]:
        self._ensure_workbook_use_workbooks_api(False)
        op_code = UsOpCode.GET_FOLDER_CONTENT
        entries_per_page = 100

        base_req = Req(
            method="get",
            url="v1/navigation",
            params=dict(
                pageSize=str(entries_per_page),
                path=us_path,
                includePermissionsInfo="false",
            ),
            require_ok=False,
        )
        loaded_entry_dict_list: list[dict[str, Any]] = []
        current_page_token: str = "0"

        # Loading all entries in folder per page
        while True:
            req = attr.evolve(base_req, params=dict(base_req.params, page=current_page_token))
            resp = await self.make_request(req)

            if resp.status != 200:
                self.raise_from_resp(resp, op_code=op_code)

            resp_json = resp.json

            loaded_entry_dict_list.extend(resp_json["entries"])
            next_page_token = resp_json.get("nextPageToken")

            if next_page_token is None:
                break
            else:
                current_page_token = next_page_token

        # Converting dicts to EntrySummary
        ret: list[EntrySummary] = []

        for entry_dict in loaded_entry_dict_list:
            entry_scope_str = entry_dict["scope"]
            try:
                scope = EntryScope[entry_scope_str]
            except KeyError:
                continue

            ret.append(
                EntrySummary.from_key(
                    entry_id=entry_dict["entryId"],
                    key=entry_dict["key"],
                    scope=scope,
                    true_workbook=False,
                )
            )

        return frozenset(ret)

    async def create_workbook(self, title: str) -> str:
        self._ensure_workbook_use_workbooks_api(True)
        return await self._us_workbook_cmd_client.create_workbook(title)

    async def delete_workbook(self, wb_id: str) -> None:
        self._ensure_workbook_use_workbooks_api(True)
        return await self._us_workbook_cmd_client.delete_workbook(wb_id)

    async def get_workbook_basic_info(self, wb_id: str) -> WorkbookBasicInfo:
        self._ensure_workbook_use_workbooks_api(True)
        op_code = UsOpCode.WB_BASIC_INFO_GET

        req = Req(method="get", url=f"v2/workbooks/{self.validate_id(wb_id)}", require_ok=False)
        resp = await self.make_request(req)

        if resp.status != 200:
            self.raise_from_resp(resp, op_code=op_code)

        return WorkbookBasicInfo(
            id=wb_id, title=resp.json["title"], project_id=self._us_workbook_cmd_client._tenant.get_tenant_id()
        )

    async def get_workbook_backend_toc(self, wb_id: str) -> WorkbookBackendTOC:
        self._ensure_workbook_use_workbooks_api(True)
        op_code = UsOpCode.WB_INFO_GET

        req = Req(method="get", url=f"v2/workbooks/{self.validate_id(wb_id)}/entries", require_ok=False)
        resp = await self.make_request(req)

        if resp.status != 200:
            self.raise_from_resp(resp, op_code=op_code)

        entry_dict_list = resp.json["entries"]

        map_scope_summary_list: dict[EntryScope, list[EntrySummary]] = {}
        connection_summary_list: list[BIConnectionSummary] = []

        for entry_dict in entry_dict_list:
            scope_str = entry_dict["scope"]
            scope = EntryScope[scope_str]

            entry_summary = EntrySummary(
                id=entry_dict["entryId"], name=entry_dict["key"].split("/")[-1], scope=scope, workbook_id=wb_id
            )
            map_scope_summary_list.setdefault(scope, []).append(entry_summary)

            if scope == EntryScope.connection:
                connection_summary_list.append(
                    BIConnectionSummary(
                        common_summary=entry_summary,
                        type=ConnectionType[entry_dict["type"]],
                    )
                )

        return WorkbookBackendTOC(
            datasets=frozenset(map_scope_summary_list.get(EntryScope.dataset, [])),
            charts=frozenset(map_scope_summary_list.get(EntryScope.widget, [])),
            dashboards=frozenset(map_scope_summary_list.get(EntryScope.dash, [])),
            connections=frozenset(connection_summary_list),
        )

    async def private_get_workbook_config(self, workbook_id: str) -> dict[str, Any]:
        self._ensure_workbook_use_workbooks_api(True)
        op_code = UsOpCode.WB_PRIVATE_INFO_GET

        req = Req(method="get", url=f"private/v2/workbooks/{self.validate_id(workbook_id)}", require_ok=False)
        resp = await self.make_request(req)

        if resp.status == 400:
            body = resp.json
            if isinstance(body, dict) and body.get("code") == "DECODE_ID_FAILED":
                raise InvalidIDFormatError()

        if resp.status != 200:
            self.raise_from_resp(resp, op_code=op_code)

        return resp.json

    async def get_collection_contents(
        self,
        collection_id: Optional[str],
        collections_page: Optional[str],
        workbooks_page: Optional[str],
        page_size: int,
    ) -> CollectionContentsPage:
        self._ensure_workbook_use_workbooks_api(True)
        op_code = UsOpCode.COLLECTION_CONTENT_GET

        params = {
            "collectionId": collection_id,
            "collectionsPage": collections_page,
            "workbooksPage": workbooks_page,
            "pageSize": page_size,
        }
        req = Req(
            method="get",
            url="/v1/collection-content",
            params={k: v for k, v in params.items() if v is not None},
            require_ok=False,
        )
        resp = await self.make_request(req)

        if resp.status != 200:
            self.raise_from_resp(resp, op_code=op_code)

        collections = [CollectionInfo(c["collectionId"]) for c in resp.json.get("collections", [])]
        collections_next_page_token = resp.json.get("collectionsNextPageToken")
        workbooks = [
            WorkbookInCollectionInfo(id=wb["workbookId"], title=wb["title"]) for wb in resp.json.get("workbooks", [])
        ]
        workbooks_next_page_token = resp.json.get("workbooksNextPageToken")
        return CollectionContentsPage(
            collections=collections,
            collections_next_page_token=collections_next_page_token,
            workbooks=workbooks,
            workbooks_next_page_token=workbooks_next_page_token,
        )

    async def private_resolve_project_id_by_wb_id(self, workbook_id: str) -> str:
        wb_data = await self.private_get_workbook_config(workbook_id)
        return wb_data["projectId"]

    async def private_resolve_org_id_by_wb_id(self, workbook_id: str) -> str:
        org_prefix = "org_"

        wb_data = await self.private_get_workbook_config(workbook_id)
        tenant_id: str = wb_data["tenantId"]

        if tenant_id.startswith(org_prefix):
            return tenant_id.removeprefix(org_prefix)
        else:
            raise ValueError(f"Tenant ID for workbook {workbook_id!r} not start from {org_prefix!r}: {tenant_id}")
