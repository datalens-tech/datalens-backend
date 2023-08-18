import functools
from typing import ClassVar, Optional, Sequence

import attr

from bi_external_api.attrs_model_mapper import ModelDescriptor
from bi_external_api.enums import ExtAPIType
from .common import EntryInfo, Secret, EntryWBRef
from .dataset_main import Dataset
from .rpc import (
    ParticularAPIOperationTranslator,
    #
    WorkbookOpKind,
    WorkbookOpRequest,
    WorkbookOpResponse,
    #
    ModificationPlan,
    #
    WorkbookReadRequest,
    WorkbookReadResponse,
    #
    TrueWorkbookCreateResponse,
    TrueWorkbookCreateRequest,
    #
    WorkbookWriteRequest,
    WorkbookWriteResponse,
    #
    WorkbookDeleteRequest,
    WorkbookDeleteResponse,
    #
    AdviseDatasetFieldsRequest,
    AdviseDatasetFieldsResponse,
    #
    ConnectionGetResponse,
    ConnectionGetRequest,
    #
    ConnectionCreateRequest,
    ConnectionCreateResponse,
    #
    ConnectionModifyRequest,
    ConnectionModifyResponse,
    #
    ConnectionDeleteRequest,
    ConnectionDeleteResponse,
    #
    WorkbookListRequest,
    WorkbookListResponse,
)
from .workbook import WorkBook, ConnectionInstance, WorkbookIndexItem


@ModelDescriptor(api_types=[ExtAPIType.DC], is_abstract=True, children_type_discriminator_attr_name="kind")
@attr.s(frozen=True)
class DCOpRequest:
    kind: ClassVar[WorkbookOpKind]

    @classmethod
    def get_operation_kind(cls) -> WorkbookOpKind:
        return cls.kind

    def get_target_workbook_id(self) -> Optional[str]:
        return None

    def get_target_project_id(self) -> Optional[str]:
        return None


@ModelDescriptor(api_types=[ExtAPIType.DC], is_abstract=True, children_type_discriminator_attr_name="kind")
@attr.s(frozen=True)
class DCOpResponse:
    kind: ClassVar[WorkbookOpKind]


#
# Workbook read
#
@ModelDescriptor(api_types=[ExtAPIType.DC])
@attr.s(frozen=True)
class DCOpWorkbookGetRequest(DCOpRequest):
    kind = WorkbookOpKind.wb_read

    workbook_id: str = attr.ib()

    def get_target_workbook_id(self) -> Optional[str]:
        return self.workbook_id


@ModelDescriptor(api_types=[ExtAPIType.DC])
@attr.s(frozen=True)
class DCOpWorkbookGetResponse(DCOpResponse):
    kind = WorkbookOpKind.wb_read

    workbook: WorkBook = attr.ib()
    id: Optional[str] = attr.ib(default=None)
    title: Optional[str] = attr.ib(default=None)
    project_id: Optional[str] = attr.ib(default=None)


#
# Workbook create
#
@ModelDescriptor(api_types=[ExtAPIType.DC])
@attr.s(frozen=True)
class DCOpWorkbookCreateRequest(DCOpRequest):
    kind = WorkbookOpKind.wb_create

    project_id: str = attr.ib()
    workbook_title: str = attr.ib()

    def get_target_project_id(self) -> Optional[str]:
        return self.project_id


@ModelDescriptor(api_types=[ExtAPIType.DC])
@attr.s(frozen=True)
class DCOpWorkbookCreateResponse(DCOpResponse):
    kind = WorkbookOpKind.wb_create

    workbook_id: str = attr.ib()

    def get_target_workbook_id(self) -> Optional[str]:
        return self.workbook_id


#
# Workbook modify
#
@ModelDescriptor(api_types=[ExtAPIType.DC])
@attr.s(frozen=True)
class DCOpWorkbookModifyRequest(DCOpRequest):
    kind = WorkbookOpKind.wb_modify

    workbook_id: str = attr.ib()
    workbook: WorkBook = attr.ib()
    force_rewrite: Optional[bool] = attr.ib(default=None)

    def get_target_workbook_id(self) -> Optional[str]:
        return self.workbook_id


@ModelDescriptor(api_types=[ExtAPIType.DC])
@attr.s(frozen=True)
class DCOpWorkbookModifyResponse(DCOpResponse):
    kind = WorkbookOpKind.wb_modify

    executed_plan: ModificationPlan = attr.ib()
    workbook: WorkBook = attr.ib()


#
# Workbook delete
#
@ModelDescriptor(api_types=[ExtAPIType.DC])
@attr.s(frozen=True)
class DCOpWorkbookDeleteRequest(DCOpRequest):
    kind = WorkbookOpKind.wb_delete

    workbook_id: str = attr.ib()

    def get_target_workbook_id(self) -> Optional[str]:
        return self.workbook_id


@ModelDescriptor(api_types=[ExtAPIType.DC])
@attr.s(frozen=True)
class DCOpWorkbookDeleteResponse(DCOpResponse):
    kind = WorkbookOpKind.wb_delete


#
# Connection get
#
@ModelDescriptor(api_types=[ExtAPIType.DC])
@attr.s(frozen=True)
class DCOpConnectionGetRequest(DCOpRequest):
    kind = WorkbookOpKind.connection_get

    workbook_id: str = attr.ib()
    name: str = attr.ib()

    def get_target_workbook_id(self) -> Optional[str]:
        return self.workbook_id


@ModelDescriptor(api_types=[ExtAPIType.DC])
@attr.s(frozen=True)
class DCOpConnectionGetResponse(DCOpResponse):
    kind = WorkbookOpKind.connection_get

    connection: ConnectionInstance = attr.ib()


#
# Connection create
#
@ModelDescriptor(api_types=[ExtAPIType.DC])
@attr.s(frozen=True)
class DCOpConnectionCreateRequest(DCOpRequest):
    kind = WorkbookOpKind.connection_create

    workbook_id: str = attr.ib()
    connection: ConnectionInstance = attr.ib()
    secret: Optional[Secret] = attr.ib()

    def get_target_workbook_id(self) -> Optional[str]:
        return self.workbook_id


@ModelDescriptor(api_types=[ExtAPIType.DC])
@attr.s(frozen=True)
class DCOpConnectionCreateResponse(DCOpResponse):
    kind = WorkbookOpKind.connection_create

    connection_info: EntryInfo = attr.ib()


#
# Connection modify
#
@ModelDescriptor(api_types=[ExtAPIType.DC])
@attr.s(frozen=True)
class DCOpConnectionModifyRequest(DCOpRequest):
    kind = WorkbookOpKind.connection_modify

    workbook_id: str = attr.ib()
    connection: ConnectionInstance = attr.ib()
    secret: Optional[Secret] = attr.ib()

    def get_target_workbook_id(self) -> Optional[str]:
        return self.workbook_id


@ModelDescriptor(api_types=[ExtAPIType.DC])
@attr.s(frozen=True)
class DCOpConnectionModifyResponse(DCOpResponse):
    kind = WorkbookOpKind.connection_modify


#
# Connection delete
#
@ModelDescriptor(api_types=[ExtAPIType.DC])
@attr.s(frozen=True)
class DCOpConnectionDeleteRequest(DCOpRequest):
    kind = WorkbookOpKind.connection_delete

    workbook_id: str = attr.ib()
    name: str = attr.ib()

    def get_target_workbook_id(self) -> Optional[str]:
        return self.workbook_id


@ModelDescriptor(api_types=[ExtAPIType.DC])
@attr.s(frozen=True)
class DCOpConnectionDeleteResponse(DCOpResponse):
    kind = WorkbookOpKind.connection_delete


#
# Advise dataset fields
#
@ModelDescriptor(api_types=[ExtAPIType.DC])
@attr.s(frozen=True)
class DCOpAdviseDatasetFieldsRequest(DCOpRequest):
    kind = WorkbookOpKind.advise_dataset_fields

    workbook_id: str = attr.ib()
    connection_name: str = attr.ib()
    partial_dataset: Dataset = attr.ib()

    def get_target_workbook_id(self) -> Optional[str]:
        return self.workbook_id


@ModelDescriptor(api_types=[ExtAPIType.DC])
@attr.s(frozen=True)
class DCOpAdviseDatasetFieldsResponse(DCOpResponse):
    kind = WorkbookOpKind.advise_dataset_fields

    dataset: Dataset = attr.ib()


@ModelDescriptor(api_types=[ExtAPIType.DC])
@attr.s(frozen=True)
class DCOpWorkbookListRequest(DCOpRequest):
    kind = WorkbookOpKind.wb_list

    project_id: str = attr.ib()

    def get_target_project_id(self) -> Optional[str]:
        return self.project_id


@ModelDescriptor(api_types=[ExtAPIType.DC])
@attr.s(frozen=True)
class DCOpWorkbookListResponse(DCOpResponse):
    kind = WorkbookOpKind.wb_list

    workbooks: Sequence[WorkbookIndexItem] = attr.ib()


#
# Translator
#
class DoubleCloudPublicAPIOperationTranslator(ParticularAPIOperationTranslator[DCOpRequest, DCOpResponse]):
    @functools.singledispatchmethod
    def translate_op_rq(self, op_rq: DCOpRequest) -> WorkbookOpRequest:
        raise NotImplementedError()

    # TODO FIX: Try to remove (type: ignore) after MyPy upgrade
    @functools.singledispatchmethod
    def translate_op_rs(self, op_rs: WorkbookOpResponse) -> DCOpResponse:  # type: ignore
        raise NotImplementedError()

    # Read
    @translate_op_rq.register
    def translate_op_rq_wb_get(self, op_rq: DCOpWorkbookGetRequest) -> WorkbookReadRequest:
        return WorkbookReadRequest(
            workbook_id=op_rq.workbook_id,
            use_id_formula=True,
        )

    @translate_op_rs.register
    def translate_op_rs_wb_get(self, op_rs: WorkbookReadResponse) -> DCOpWorkbookGetResponse:
        return DCOpWorkbookGetResponse(
            workbook=op_rs.workbook,
            id=op_rs.id,
            title=op_rs.title,
            project_id=op_rs.project_id
        )

    # Create
    @translate_op_rq.register
    def translate_op_rq_wb_create(self, op_rq: DCOpWorkbookCreateRequest) -> TrueWorkbookCreateRequest:
        return TrueWorkbookCreateRequest(
            workbook_title=op_rq.workbook_title,
        )

    @translate_op_rs.register
    def translate_op_rs_wb_create(self, op_rs: TrueWorkbookCreateResponse) -> DCOpWorkbookCreateResponse:
        return DCOpWorkbookCreateResponse(
            workbook_id=op_rs.workbook_id,
        )

    # Modify
    @translate_op_rq.register
    def translate_op_rq_wb_modify(self, op_rq: DCOpWorkbookModifyRequest) -> WorkbookWriteRequest:
        return WorkbookWriteRequest(
            workbook_id=op_rq.workbook_id,
            workbook=op_rq.workbook,
            force_rewrite=op_rq.force_rewrite,
        )

    @translate_op_rs.register
    def translate_op_rs_wb_modify(self, op_rs: WorkbookWriteResponse) -> DCOpWorkbookModifyResponse:
        return DCOpWorkbookModifyResponse(
            executed_plan=op_rs.executed_plan,
            workbook=op_rs.workbook,
        )

    # Delete
    @translate_op_rq.register
    def translate_op_rq_wb_delete(self, op_rq: DCOpWorkbookDeleteRequest) -> WorkbookDeleteRequest:
        return WorkbookDeleteRequest(
            workbook_id=op_rq.workbook_id,
        )

    @translate_op_rs.register
    def translate_op_rs_wb_delete(self, op_rs: WorkbookDeleteResponse) -> DCOpWorkbookDeleteResponse:
        return DCOpWorkbookDeleteResponse(
        )

    # Connection get
    @translate_op_rq.register
    def translate_op_rq_connection_get(self, op_rq: DCOpConnectionGetRequest) -> ConnectionGetRequest:
        return ConnectionGetRequest(
            workbook_id=op_rq.workbook_id,
            name=op_rq.name,
        )

    @translate_op_rs.register
    def translate_op_rs_connection_get(self, op_rs: ConnectionGetResponse) -> DCOpConnectionGetResponse:
        return DCOpConnectionGetResponse(
            connection=op_rs.connection,
        )

    # Connection create
    @translate_op_rq.register
    def translate_op_rq_connection_create(self, op_rq: DCOpConnectionCreateRequest) -> ConnectionCreateRequest:
        return ConnectionCreateRequest(
            workbook_id=op_rq.workbook_id,
            connection=op_rq.connection,
            secret=op_rq.secret,
        )

    @translate_op_rs.register
    def translate_op_rs_connection_create(self, op_rs: ConnectionCreateResponse) -> DCOpConnectionCreateResponse:
        return DCOpConnectionCreateResponse(
            connection_info=op_rs.connection_info,
        )

    # Connection modify
    @translate_op_rq.register
    def translate_op_rq_connection_modify(self, op_rq: DCOpConnectionModifyRequest) -> ConnectionModifyRequest:
        return ConnectionModifyRequest(
            workbook_id=op_rq.workbook_id,
            connection=op_rq.connection,
            secret=op_rq.secret,
        )

    @translate_op_rs.register
    def translate_op_rs_connection_modify(self, op_rs: ConnectionModifyResponse) -> DCOpConnectionModifyResponse:
        return DCOpConnectionModifyResponse(
        )

    # Connection delete
    @translate_op_rq.register
    def translate_op_rq_connection_delete(self, op_rq: DCOpConnectionDeleteRequest) -> ConnectionDeleteRequest:
        return ConnectionDeleteRequest(
            workbook_id=op_rq.workbook_id,
            name=op_rq.name,
        )

    @translate_op_rs.register
    def translate_op_rs_connection_delete(self, op_rs: ConnectionDeleteResponse) -> DCOpConnectionDeleteResponse:
        return DCOpConnectionDeleteResponse()

    # Advise dataset fields
    @translate_op_rq.register
    def translate_op_rq_advise_dataset_fields(
            self,
            op_rq: DCOpAdviseDatasetFieldsRequest
    ) -> AdviseDatasetFieldsRequest:
        return AdviseDatasetFieldsRequest(
            connection_ref=EntryWBRef(
                wb_id=op_rq.workbook_id,
                entry_name=op_rq.connection_name,
            ),
            partial_dataset=op_rq.partial_dataset,
        )

    @translate_op_rs.register
    def translate_op_rs_advise_dataset_fields(
            self,
            op_rs: AdviseDatasetFieldsResponse,
    ) -> DCOpAdviseDatasetFieldsResponse:
        return DCOpAdviseDatasetFieldsResponse(
            dataset=op_rs.dataset,
        )

    # Workbooks list
    @translate_op_rq.register
    def translate_op_rq_workbook_list(self, op_rq: DCOpWorkbookListRequest) -> WorkbookListRequest:
        return WorkbookListRequest(
            project_id=op_rq.project_id
        )

    @translate_op_rs.register
    def translate_op_rs_workbook_list(self, op_rs: WorkbookListResponse) -> DCOpWorkbookListResponse:
        return DCOpWorkbookListResponse(
            workbooks=op_rs.workbooks,
        )
