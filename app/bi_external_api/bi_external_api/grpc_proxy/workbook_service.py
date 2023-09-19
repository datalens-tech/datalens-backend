from functools import wraps
import json
import logging
from typing import (
    Any,
    Callable,
    Dict,
    Optional,
)
from uuid import uuid4

import attr
from doublecloud.v1.operation_pb2 import Operation
from doublecloud.visualization.v1 import workbook_pb2 as wb_pb2
from doublecloud.visualization.v1 import workbook_service_pb2 as ws_pb2
from doublecloud.visualization.v1.workbook_service_pb2_grpc import WorkbookServiceServicer
from google.protobuf import json_format
from google.protobuf.timestamp_pb2 import Timestamp  # noqa
import grpc

from bi_external_api.grpc_proxy.common import GRequestData
from bi_external_api.grpc_proxy.ext_api_client import (
    CallError,
    ExtApiClient,
    Response,
)


LOGGER = logging.getLogger(__name__)


def err_handle(func: Callable) -> Callable:
    @wraps(func)
    def wrapped(self: "WorkbookService", request: Any, context: grpc.ServicerContext) -> Callable:  # type: ignore
        try:
            return func(self, request, context)
        except CallError as err:
            self._ext_api_error(context, err.response, err)
        except Exception as err:
            LOGGER.exception(f"Internal exception during {func.__name__}")
            self._ext_api_error(context, None, err)

        # should never end up here
        self._ext_api_error(context, None, None)

    return wrapped


@attr.s
class WorkbookService(WorkbookServiceServicer):
    _ext_api_client: ExtApiClient = attr.ib()

    def get_ext_api_client_for_current_request(self, ctx: grpc.ServicerContext) -> ExtApiClient:
        return attr.evolve(
            self._ext_api_client,
            request_id=GRequestData.get_request_id(ctx),
            authorization_header=GRequestData.get_auth_data(ctx).authorization_header,
        )

    def _op(self, params: Optional[dict] = None) -> Operation:  # type: ignore
        if params is None:
            params = dict()

        current_time = Timestamp()
        current_time.GetCurrentTime()

        effective = dict(
            id=uuid4().hex,  # noqa
            project_id="",
            resource_id="",
            description="",
            created_by="",
            create_time=current_time,
            start_time=current_time,
            finish_time=current_time,
            status=Operation.Status.STATUS_DONE,  # type: ignore
            error=None,
            metadata=dict(),
        )
        effective.update(params)
        op = Operation(**effective)
        return op

    # CODE_MAPPING = {
    #     400: code_pb2.INVALID_ARGUMENT,
    #     401: code_pb2.UNAUTHENTICATED,  # not sure if we actually pass 401, 403 from our api
    #     403: code_pb2.PERMISSION_DENIED,
    #     404: code_pb2.NOT_FOUND,
    # }

    # This text in application error field `message`
    #  indicates that workbook was not found
    WORKBOOK_NOT_FOUND_ERR_MSG = "Workbook was not found"
    WORKBOOK_ACCESS_DENIED = "No access to workbook"

    def _ext_api_error(
        self,
        context: grpc.ServicerContext,
        response: Optional[Response] = None,
        err: Optional[Exception] = None,
    ) -> None:
        # aiming for the same structure as in ..

        details: Dict[str, Any] = {
            "message": "Unexpected error",
            "request_id": GRequestData().get_request_id(context),
            "common_errors": [],
            "entry_errors": [],
            "partial_workbook": None,
        }

        code = grpc.StatusCode.INTERNAL
        if response and 400 <= response.status_code < 500:
            code = grpc.StatusCode.INVALID_ARGUMENT
            if response.json_resp_safe:
                if response.json_resp_safe.get("message") == self.WORKBOOK_NOT_FOUND_ERR_MSG:
                    code = grpc.StatusCode.NOT_FOUND
                    details["message"] = self.WORKBOOK_NOT_FOUND_ERR_MSG
                elif response.json_resp_safe.get("message") == self.WORKBOOK_ACCESS_DENIED:
                    code = grpc.StatusCode.PERMISSION_DENIED
                    details["message"] = self.WORKBOOK_ACCESS_DENIED
            else:
                details["message"] = "Bad request"

        if response and response.json_resp_safe:
            for field in ["common_errors", "entry_errors", "partial_workbook", "message"]:
                if response.json_resp_safe.get(field):
                    details[field] = response.json_resp_safe[field]

        if not response and err:
            details["message"] = "Unexpected error occurred during internal call"
            details["common_errors"] = [
                dict(
                    path=None,
                    message=str(err),
                    exc_message=None,  # not exposing internals, at least for now
                    stacktrace=None,
                )
            ]

        context.abort(code, details=json.dumps(details))

    def _self_err(self, context: grpc.ServicerContext) -> None:
        context.abort(grpc.StatusCode.INTERNAL, details={"message": "Unexpected error in the grpc service occurred."})

    @err_handle
    def Get(
        self,
        request: ws_pb2.GetWorkbookRequest,  # type: ignore
        context: grpc.ServicerContext,
    ) -> ws_pb2.GetWorkbookResponse:  # type: ignore
        result = self.get_ext_api_client_for_current_request(context).get_workbook(request.workbook_id)  # type: ignore

        wb = wb_pb2.Workbook()
        wb.config.struct_value.update(result.json_resp_safe.get("workbook"))
        response = ws_pb2.GetWorkbookResponse(workbook=wb)

        return response

    @err_handle
    def Create(
        self,
        request: ws_pb2.CreateWorkbookRequest,  # type: ignore
        context: grpc.ServicerContext,
    ) -> Operation:  # type: ignore
        result = self.get_ext_api_client_for_current_request(context).create_workbook(
            project_id=request.project_id,  # type: ignore
            workbook_title=request.workbook_title,  # type: ignore
        )  # type: ignore
        return self._op({"resource_id": result.json_resp_safe.get("workbook_id")})

    @err_handle
    def Update(
        self,
        request: ws_pb2.UpdateWorkbookRequest,  # type: ignore
        context: grpc.ServicerContext,
    ) -> Operation:  # type: ignore
        workbook = json_format.MessageToDict(request.workbook.config)  # type: ignore

        self.get_ext_api_client_for_current_request(context).update_workbook(
            request.workbook_id,  # type: ignore
            workbook,
        )

        return self._op({"resource_id": request.workbook_id})  # type: ignore

    @err_handle
    def Delete(
        self,
        request: ws_pb2.DeleteWorkbookRequest,  # type: ignore
        context: grpc.ServicerContext,
    ) -> Operation:  # type: ignore
        self.get_ext_api_client_for_current_request(context).delete_workbook(
            request.workbook_id,  # type: ignore
        )

        return self._op({"resource_id": request.workbook_id})  # type: ignore

    @err_handle
    def GetConnection(
        self,
        request: ws_pb2.GetWorkbookConnectionRequest,  # type: ignore
        context: grpc.ServicerContext,
    ) -> ws_pb2.GetWorkbookConnectionResponse:  # type: ignore
        result = self.get_ext_api_client_for_current_request(context).get_connection(
            request.workbook_id,  # type: ignore
            request.connection_name,  # type: ignore
        )

        connection = wb_pb2.Connection()
        connection.config.struct_value.update(result.json_resp_safe.get("connection"))
        response = ws_pb2.GetWorkbookConnectionResponse(connection=connection)
        return response

    def _fix_secret_dict(self, src: dict) -> Optional[dict]:
        """
        Plain secret:
            {'plainSecret': {'secret': 'xxx'}}

        :param src:
        :return:
        """
        if "plainSecret" in src:
            return dict(
                kind="plain",
                secret=src["plainSecret"].get("secret"),
            )

        return None

    @err_handle
    def CreateConnection(
        self,
        request: ws_pb2.CreateWorkbookConnectionRequest,  # type: ignore
        context: grpc.ServicerContext,
    ) -> Operation:  # type: ignore
        connection_params = json_format.MessageToDict(request.connection.config)  # type: ignore
        secret = json_format.MessageToDict(request.secret)  # type: ignore
        secret = self._fix_secret_dict(secret)  # type: ignore

        self.get_ext_api_client_for_current_request(context).create_connection(
            workbook_id=request.workbook_id,  # type: ignore
            connection_name=request.connection_name,  # type: ignore
            secret=secret,
            connection_params=connection_params,
        )

        return self._op({"resource_id": request.workbook_id})  # type: ignore

    @err_handle
    def UpdateConnection(
        self,
        request: ws_pb2.UpdateWorkbookConnectionRequest,  # type: ignore
        context: grpc.ServicerContext,
    ) -> Operation:  # type: ignore
        connection_params = json_format.MessageToDict(request.connection.config)  # type: ignore
        secret = json_format.MessageToDict(request.secret)  # type: ignore
        secret = self._fix_secret_dict(secret)  # type: ignore

        self.get_ext_api_client_for_current_request(context).update_connection(
            workbook_id=request.workbook_id,  # type: ignore
            connection_name=request.connection_name,  # type: ignore
            secret=secret,
            connection_params=connection_params,  # type: ignore
        )

        return self._op({"resource_id": request.workbook_id})  # type: ignore

    @err_handle
    def DeleteConnection(
        self,
        request: ws_pb2.DeleteWorkbookConnectionRequest,  # type: ignore
        context: grpc.ServicerContext,
    ) -> Operation:  # type: ignore
        self.get_ext_api_client_for_current_request(context).delete_connection(
            request.workbook_id,  # type: ignore
            request.connection_name,  # type: ignore
        )

        return self._op({"resource_id": request.workbook_id})  # type: ignore

    @err_handle
    def AdviseDatasetFields(
        self,
        request: ws_pb2.AdviseDatasetFieldsRequest,  # type: ignore
        context: grpc.ServicerContext,
    ) -> ws_pb2.AdviseDatasetFieldsResponse:  # type: ignore
        pds = json_format.MessageToDict(request.partial_dataset.config)  # type: ignore

        result = self.get_ext_api_client_for_current_request(context).advice_dataset_fields(
            workbook_id=request.workbook_id,  # type: ignore
            connection_name=request.connection_name,  # type: ignore
            partial_dataset=pds,
        )

        ds = wb_pb2.Dataset()
        ds.config.struct_value.update(result.json_resp_safe.get("dataset"))
        return ws_pb2.AdviseDatasetFieldsResponse(dataset=ds)

    @err_handle
    def ListWorkbooks(
        self,
        request: ws_pb2.ListWorkbooksRequest,  # type: ignore
        context: grpc.ServicerContext,
    ) -> ws_pb2.ListWorkbooksResponse:  # type: ignore
        result = self.get_ext_api_client_for_current_request(context).list_workbooks(
            project_id=request.project_id  # type: ignore
        )

        workbooks = result.json_resp_safe.get("workbooks")
        return ws_pb2.ListWorkbooksResponse(workbooks=[map_workbook_index_item(wb) for wb in workbooks])  # type: ignore


def map_workbook_index_item(wb_as_json: dict[str, Any]) -> wb_pb2.WorkbooksIndexItem:  # type: ignore
    return wb_pb2.WorkbooksIndexItem(
        id=wb_as_json["id"],
        title=wb_as_json["title"],
    )
