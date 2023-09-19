from contextlib import contextmanager
from typing import (
    Any,
    Generator,
    List,
    Optional,
    Tuple,
)
from uuid import uuid4

import attr
from doublecloud.visualization.v1 import workbook_pb2
from doublecloud.visualization.v1 import workbook_service_pb2 as ws_pb2
from doublecloud.visualization.v1 import workbook_service_pb2_grpc as ws_grpc_pb2
from google.protobuf import (
    json_format,
    wrappers_pb2,
)
import grpc

from bi_external_api.grpc_proxy.common import GHeaders


try:
    from src.proto.grpc.health.v1 import health_pb2  # type: ignore
    from src.proto.grpc.health.v1 import health_pb2_grpc  # type: ignore
except ImportError:
    from grpc_health.v1 import health_pb2  # type: ignore
    from grpc_health.v1 import health_pb2_grpc  # type: ignore


@attr.s(kw_only=True)  # no idea why mypy reacts with "function is missing a type annotation for one or more arguments"
class GrpcClientCtx:  # type: ignore
    """
    Expected to be used for the test purposes,
    hence .do_call method converts response back to python structures by defualt
    """

    endpoint: str = attr.ib()
    channel_credentials: Optional[grpc.ServerCredentials] = attr.ib()
    headers: GHeaders = attr.ib()

    def headers_to_metadata(self, call_headers: Optional[dict] = None) -> List[Tuple[str, str]]:
        headers = GHeaders(**self.headers)  # noqa
        if call_headers:
            headers.update(call_headers)

        return headers.to_metadata()

    @contextmanager
    def make_channel(
        self, call_credential: Optional[grpc.ServerCredentials] = None
    ) -> Generator[grpc.Channel, None, None]:
        creds = [c for c in (self.channel_credentials, call_credential) if c is not None]
        if len(creds) == 0:
            yield grpc.aio.insecure_channel(
                self.endpoint,
            )
        elif len(creds) == 1:
            # TODO FIX: BI-4282 figure out why MyPy error:
            #  Argument 2 to "secure_channel" has incompatible type "ServerCredentials"; expected "ChannelCredentials"
            yield grpc.aio.secure_channel(self.endpoint, creds[0])  # type: ignore
        else:
            composite_credentials = grpc.composite_channel_credentials(*creds)
            channel = grpc.aio.secure_channel(self.endpoint, composite_credentials)
            yield channel

    @contextmanager
    def make_service(
        self, call_credential: Optional[grpc.ServerCredentials] = None
    ) -> Generator[ws_grpc_pb2.WorkbookServiceStub, None, None]:
        with self.make_channel(call_credential) as ch:  # type: grpc.Channel
            yield ws_grpc_pb2.WorkbookServiceStub(ch)

    async def do_call_raw(
        self,
        name: str,
        msg: Any,
        headers: Optional[dict] = None,
        request_id: Optional[str] = None,
    ) -> Any:
        request_id = request_id if request_id is not None else uuid4().hex
        headers_ex = {"x-request-id": request_id}
        if headers:
            headers_ex.update(headers)

        with self.make_channel() as ch:  # type: grpc.Channel
            service = ws_grpc_pb2.WorkbookServiceStub(ch)
            # try:
            result = await getattr(service, name)(msg, metadata=self.headers_to_metadata(headers))
            # except BaseException as err_info:
            #     import ipdb; ipdb.set_trace()
            #     result = None
            #     raise
            return result

    async def do_call(
        self,
        name: str,
        msg: Any,
        headers: Optional[dict] = None,
        request_id: Optional[str] = None,
    ) -> dict:
        result = await self.do_call_raw(name, msg, headers=headers, request_id=request_id)
        return json_format.MessageToDict(result)

    async def get_health_check(
        self,
    ) -> health_pb2.HealthCheckResponse:  # type: ignore
        with self.make_channel() as ch:  # type: grpc.Channel
            request = health_pb2.HealthCheckRequest(service="")

            service = health_pb2_grpc.HealthStub(ch)
            return await service.Check(request)

    async def is_alive(self) -> bool:
        health = await self.get_health_check()
        return health.status == health_pb2.HealthCheckResponse.SERVING  # type: ignore


@attr.s(frozen=True)
class VisualizationV1Client:
    ctx: GrpcClientCtx = attr.ib()

    async def is_alive(self) -> bool:
        return await self.ctx.is_alive()

    async def get_workbook(
        self,
        wb_id: str,
    ) -> dict:
        msg = ws_pb2.GetWorkbookRequest(workbook_id=wb_id)
        return await self.ctx.do_call("Get", msg)

    async def get_workbook_raw(self, wb_id: str) -> Any:
        msg = ws_pb2.GetWorkbookRequest(workbook_id=wb_id)
        return await self.ctx.do_call_raw("Get", msg)

    async def create_workbook(
        self,
        *,
        wb_title: str,
        project_id: str,
    ) -> dict:
        msg = ws_pb2.CreateWorkbookRequest(workbook_title=wb_title, project_id=project_id)
        return await self.ctx.do_call("Create", msg)

    async def update_workbook(self, wb_id: str, workbook: dict, force_rewrite: Optional[bool] = None) -> dict:
        wb = workbook_pb2.Workbook()
        wb.config.struct_value.update(workbook)

        if force_rewrite is not None:
            fr = wrappers_pb2.BoolValue(value=force_rewrite)  # type: ignore
        else:
            fr = None  # type: ignore

        msg = ws_pb2.UpdateWorkbookRequest(
            workbook_id=wb_id,
            workbook=wb,
            force_rewrite=fr,
        )
        return await self.ctx.do_call("Update", msg)

    async def delete_workbook(self, wb_id: str) -> dict:
        msg = ws_pb2.DeleteWorkbookRequest(workbook_id=wb_id)
        return await self.ctx.do_call("Delete", msg)

    async def create_connection(
        self,
        wb_id: str,
        name: str,
        plain_secret: Optional[str],
        connection_params: dict,
    ) -> dict:
        secret = None
        if plain_secret:
            secret = workbook_pb2.Secret(
                plain_secret=workbook_pb2.PlainSecret(secret=plain_secret),
            )
        conn = workbook_pb2.Connection()
        conn.config.struct_value.update(connection_params)
        msg_params = dict(
            workbook_id=wb_id,
            connection_name=name,
            connection=conn,
        )
        if secret:
            msg_params["secret"] = secret
        msg = ws_pb2.CreateWorkbookConnectionRequest(**msg_params)
        return await self.ctx.do_call("CreateConnection", msg)

    async def update_connection(
        self,
        wb_id: str,
        name: str,
        plain_secret: str,
        connection_params: dict,
    ) -> dict:
        secret = workbook_pb2.Secret(
            plain_secret=workbook_pb2.PlainSecret(secret=plain_secret),
        )
        connection = workbook_pb2.Connection()
        connection.config.struct_value.update(connection_params)
        msg = ws_pb2.UpdateWorkbookConnectionRequest(
            workbook_id=wb_id,
            connection_name=name,
            secret=secret,
            connection=connection,
        )
        return await self.ctx.do_call("UpdateConnection", msg)

    async def get_connection(
        self,
        wb_id: str,
        name: str,
    ) -> dict:
        msg = ws_pb2.GetWorkbookConnectionRequest(
            workbook_id=wb_id,
            connection_name=name,
        )
        return await self.ctx.do_call("GetConnection", msg)

    async def delete_connection(
        self,
        wb_id: str,
        name: str,
    ) -> dict:
        msg = ws_pb2.DeleteWorkbookConnectionRequest(
            workbook_id=wb_id,
            connection_name=name,
        )
        return await self.ctx.do_call("DeleteConnection", msg)

    async def advise_dataset_fields(
        self,
        wb_id: str,
        connection_name: str,
        partial_dataset: dict,
    ) -> dict:
        ds = workbook_pb2.Dataset()
        ds.config.struct_value.update(partial_dataset)
        msg = ws_pb2.AdviseDatasetFieldsRequest(
            workbook_id=wb_id,
            connection_name=connection_name,
            partial_dataset=ds,
        )
        return await self.ctx.do_call("AdviseDatasetFields", msg)

    async def list_workbooks(
        self,
        project_id: str,
    ) -> dict:
        msg = ws_pb2.ListWorkbooksRequest(project_id=project_id)
        return await self.ctx.do_call("ListWorkbooks", msg)
