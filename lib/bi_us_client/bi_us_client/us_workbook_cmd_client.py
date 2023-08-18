import attr

from bi_api_commons.client.base import Req
from bi_api_commons.client.common import CommonInternalAPIClient
from bi_us_client.constants import OpCode


@attr.s()
class USWorkbookCommandClient(CommonInternalAPIClient):

    async def create_workbook(self, title: str) -> str:
        op_code = OpCode.WB_CREATE

        req = Req(
            method="post",
            url="v2/workbooks",
            data_json=dict(
                title=title,
            ),
            require_ok=False
        )
        resp = await self.make_request(req)

        if resp.status == 200:
            return resp.json["workbookId"]

        self.raise_from_resp(resp, op_code=op_code)

    async def delete_workbook(self, wb_id: str) -> None:
        op_code = OpCode.WB_DELETE

        req = Req(
            method="delete",
            url=f"v2/workbooks/{wb_id}",
            require_ok=False
        )
        resp = await self.make_request(req)

        if resp.status == 200:
            return
        self.raise_from_resp(resp, op_code=op_code)

    async def add_wb_sa_access_binding(self, wb_id: str, sa_id: str) -> None:  # TODO: make more generic
        op_code = "wb_alter_access_bindings"

        data_json = dict(
            deltas=[
                dict(
                    action="ADD",
                    accessBinding=dict(
                        roleId="datalens.workbooks.admin",
                        subject=dict(
                            id=sa_id,
                            type="serviceAccount"
                        )
                    )
                )
            ],
        )

        req = Req(
            method="post",
            url=f"v2/workbooks/{wb_id}/access-bindings",
            data_json=data_json,
            require_ok=False
        )
        resp = await self.make_request(req)

        if resp.status != 200:
            self.raise_from_resp(resp, op_code=op_code)
