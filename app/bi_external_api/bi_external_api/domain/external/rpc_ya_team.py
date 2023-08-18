import functools
from typing import ClassVar, Optional, Sequence

import attr
from bi_external_api.domain.external import ConnectionSecret

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
    FakeWorkbookCreateResponse,
    FakeWorkbookCreateRequest,
    #
    WorkbookWriteRequest,
    WorkbookWriteResponse,
    #
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
)
from .workbook import WorkBook, ConnectionInstance, WorkbookConnectionsOnly


@ModelDescriptor(api_types=[ExtAPIType.YA_TEAM], is_abstract=True, children_type_discriminator_attr_name="kind")
@attr.s(frozen=True)
class YaTeamOpRequest:
    kind: ClassVar[WorkbookOpKind]

    @classmethod
    def get_operation_kind(cls) -> WorkbookOpKind:
        return cls.kind

    def get_target_workbook_id(self) -> Optional[str]:
        return None

    def get_target_project_id(self) -> Optional[str]:
        return None


@ModelDescriptor(api_types=[ExtAPIType.YA_TEAM], is_abstract=True, children_type_discriminator_attr_name="kind")
@attr.s(frozen=True)
class YaTeamOpResponse:
    kind: ClassVar[WorkbookOpKind]


#
# Workbook read
#
@ModelDescriptor(api_types=[ExtAPIType.YA_TEAM])
@attr.s(frozen=True)
class YaTeamOpWorkbookGetRequest(YaTeamOpRequest):
    kind = WorkbookOpKind.wb_read

    workbook_id: str = attr.ib()

    def get_target_workbook_id(self) -> Optional[str]:
        return self.workbook_id


@ModelDescriptor(api_types=[ExtAPIType.YA_TEAM])
@attr.s(frozen=True)
class YaTeamOpWorkbookGetResponse(YaTeamOpResponse):
    kind = WorkbookOpKind.wb_read

    workbook: WorkBook = attr.ib()


#
# Workbook create
#
@ModelDescriptor(api_types=[ExtAPIType.YA_TEAM])
@attr.s(frozen=True)
class YaTeamOpWorkbookCreateRequest(YaTeamOpRequest):
    kind = WorkbookOpKind.wb_create

    workbook_id: str = attr.ib()
    workbook: Optional[WorkbookConnectionsOnly] = attr.ib()
    connection_secrets: Sequence[ConnectionSecret] = attr.ib()

    def get_target_workbook_id(self) -> Optional[str]:
        return self.workbook_id


@ModelDescriptor(api_types=[ExtAPIType.YA_TEAM])
@attr.s(frozen=True)
class YaTeamOpWorkbookCreateResponse(YaTeamOpResponse):
    kind = WorkbookOpKind.wb_create

    workbook_id: str = attr.ib()
    created_entries_info: Sequence[EntryInfo] = attr.ib()

    def get_target_workbook_id(self) -> Optional[str]:
        return self.workbook_id


#
# Workbook modify
#
@ModelDescriptor(api_types=[ExtAPIType.YA_TEAM])
@attr.s(frozen=True)
class YaTeamOpWorkbookModifyRequest(YaTeamOpRequest):
    kind = WorkbookOpKind.wb_modify

    workbook_id: str = attr.ib()
    workbook: WorkBook = attr.ib()
    force_rewrite: Optional[bool] = attr.ib(default=None)

    def get_target_workbook_id(self) -> Optional[str]:
        return self.workbook_id


@ModelDescriptor(api_types=[ExtAPIType.YA_TEAM])
@attr.s(frozen=True)
class YaTeamOpWorkbookModifyResponse(YaTeamOpResponse):
    kind = WorkbookOpKind.wb_modify

    executed_plan: ModificationPlan = attr.ib()
    workbook: WorkBook = attr.ib()


#
# Connection get
#
@ModelDescriptor(api_types=[ExtAPIType.YA_TEAM])
@attr.s(frozen=True)
class YaTeamOpConnectionGetRequest(YaTeamOpRequest):
    kind = WorkbookOpKind.connection_get

    workbook_id: str = attr.ib()
    name: str = attr.ib()

    def get_target_workbook_id(self) -> Optional[str]:
        return self.workbook_id


@ModelDescriptor(api_types=[ExtAPIType.YA_TEAM])
@attr.s(frozen=True)
class YaTeamOpConnectionGetResponse(YaTeamOpResponse):
    kind = WorkbookOpKind.connection_get

    connection: ConnectionInstance = attr.ib()


#
# Connection create
#
@ModelDescriptor(api_types=[ExtAPIType.YA_TEAM])
@attr.s(frozen=True)
class YaTeamOpConnectionCreateRequest(YaTeamOpRequest):
    kind = WorkbookOpKind.connection_create

    workbook_id: str = attr.ib()
    connection: ConnectionInstance = attr.ib()
    secret: Optional[Secret] = attr.ib()

    def get_target_workbook_id(self) -> Optional[str]:
        return self.workbook_id


@ModelDescriptor(api_types=[ExtAPIType.YA_TEAM])
@attr.s(frozen=True)
class YaTeamOpConnectionCreateResponse(YaTeamOpResponse):
    kind = WorkbookOpKind.connection_create

    connection_info: EntryInfo = attr.ib()


#
# Connection modify
#
@ModelDescriptor(api_types=[ExtAPIType.YA_TEAM])
@attr.s(frozen=True)
class YaTeamOpConnectionModifyRequest(YaTeamOpRequest):
    kind = WorkbookOpKind.connection_modify

    workbook_id: str = attr.ib()
    connection: ConnectionInstance = attr.ib()
    secret: Optional[Secret] = attr.ib()

    def get_target_workbook_id(self) -> Optional[str]:
        return self.workbook_id


@ModelDescriptor(api_types=[ExtAPIType.YA_TEAM])
@attr.s(frozen=True)
class YaTeamOpConnectionModifyResponse(YaTeamOpResponse):
    kind = WorkbookOpKind.connection_modify


#
# Connection delete
#
@ModelDescriptor(api_types=[ExtAPIType.YA_TEAM])
@attr.s(frozen=True)
class YaTeamOpConnectionDeleteRequest(YaTeamOpRequest):
    kind = WorkbookOpKind.connection_delete

    workbook_id: str = attr.ib()
    name: str = attr.ib()

    def get_target_workbook_id(self) -> Optional[str]:
        return self.workbook_id


@ModelDescriptor(api_types=[ExtAPIType.YA_TEAM])
@attr.s(frozen=True)
class YaTeamOpConnectionDeleteResponse(YaTeamOpResponse):
    kind = WorkbookOpKind.connection_delete


#
# Advise dataset fields
#
@ModelDescriptor(api_types=[ExtAPIType.YA_TEAM])
@attr.s(frozen=True)
class YaTeamOpAdviseDatasetFieldsRequest(YaTeamOpRequest):
    kind = WorkbookOpKind.advise_dataset_fields

    workbook_id: str = attr.ib()
    connection_name: str = attr.ib()
    partial_dataset: Dataset = attr.ib()

    def get_target_workbook_id(self) -> Optional[str]:
        return self.workbook_id


@ModelDescriptor(api_types=[ExtAPIType.YA_TEAM])
@attr.s(frozen=True)
class YaTeamOpAdviseDatasetFieldsResponse(YaTeamOpResponse):
    kind = WorkbookOpKind.advise_dataset_fields

    dataset: Dataset = attr.ib()


#
# Translator
#
class YaTeamPublicAPIOperationTranslator(ParticularAPIOperationTranslator[YaTeamOpRequest, YaTeamOpResponse]):
    @functools.singledispatchmethod
    def translate_op_rq(self, op_rq: YaTeamOpRequest) -> WorkbookOpRequest:
        raise NotImplementedError()

    # TODO FIX: Try to remove (type: ignore) after MyPy upgrade
    @functools.singledispatchmethod
    def translate_op_rs(self, op_rs: WorkbookOpResponse) -> YaTeamOpResponse:  # type: ignore
        raise NotImplementedError()

    # Read
    @translate_op_rq.register
    def translate_op_rq_wb_get(self, op_rq: YaTeamOpWorkbookGetRequest) -> WorkbookReadRequest:
        return WorkbookReadRequest(
            workbook_id=op_rq.workbook_id,
            use_id_formula=False,
        )

    @translate_op_rs.register
    def translate_op_rs_wb_get(self, op_rs: WorkbookReadResponse) -> YaTeamOpWorkbookGetResponse:
        return YaTeamOpWorkbookGetResponse(
            workbook=op_rs.workbook,
        )

    # Create
    @translate_op_rq.register
    def translate_op_rq_wb_create(self, op_rq: YaTeamOpWorkbookCreateRequest) -> FakeWorkbookCreateRequest:
        return FakeWorkbookCreateRequest(
            workbook_id=op_rq.workbook_id,
            workbook=op_rq.workbook,
            connection_secrets=op_rq.connection_secrets
        )

    @translate_op_rs.register
    def translate_op_rs_wb_create(self, op_rs: FakeWorkbookCreateResponse) -> YaTeamOpWorkbookCreateResponse:
        return YaTeamOpWorkbookCreateResponse(
            workbook_id=op_rs.workbook_id,
            created_entries_info=op_rs.created_entries_info
        )

    # Modify
    @translate_op_rq.register
    def translate_op_rq_wb_modify(self, op_rq: YaTeamOpWorkbookModifyRequest) -> WorkbookWriteRequest:
        return WorkbookWriteRequest(
            workbook_id=op_rq.workbook_id,
            workbook=op_rq.workbook,
            force_rewrite=op_rq.force_rewrite,
        )

    @translate_op_rs.register
    def translate_op_rs_wb_modify(self, op_rs: WorkbookWriteResponse) -> YaTeamOpWorkbookModifyResponse:
        return YaTeamOpWorkbookModifyResponse(
            executed_plan=op_rs.executed_plan,
            workbook=op_rs.workbook,
        )

    # Connection get
    @translate_op_rq.register
    def translate_op_rq_connection_get(self, op_rq: YaTeamOpConnectionGetRequest) -> ConnectionGetRequest:
        return ConnectionGetRequest(
            workbook_id=op_rq.workbook_id,
            name=op_rq.name,
        )

    @translate_op_rs.register
    def translate_op_rs_connection_get(self, op_rs: ConnectionGetResponse) -> YaTeamOpConnectionGetResponse:
        return YaTeamOpConnectionGetResponse(
            connection=op_rs.connection,
        )

    # Connection create
    @translate_op_rq.register
    def translate_op_rq_connection_create(self, op_rq: YaTeamOpConnectionCreateRequest) -> ConnectionCreateRequest:
        return ConnectionCreateRequest(
            workbook_id=op_rq.workbook_id,
            connection=op_rq.connection,
            secret=op_rq.secret,
        )

    @translate_op_rs.register
    def translate_op_rs_connection_create(self, op_rs: ConnectionCreateResponse) -> YaTeamOpConnectionCreateResponse:
        return YaTeamOpConnectionCreateResponse(
            connection_info=op_rs.connection_info,
        )

    # Connection modify
    @translate_op_rq.register
    def translate_op_rq_connection_modify(self, op_rq: YaTeamOpConnectionModifyRequest) -> ConnectionModifyRequest:
        return ConnectionModifyRequest(
            workbook_id=op_rq.workbook_id,
            connection=op_rq.connection,
            secret=op_rq.secret,
        )

    @translate_op_rs.register
    def translate_op_rs_connection_modify(self, op_rs: ConnectionModifyResponse) -> YaTeamOpConnectionModifyResponse:
        return YaTeamOpConnectionModifyResponse(
        )

    # Connection delete
    @translate_op_rq.register
    def translate_op_rq_connection_delete(self, op_rq: YaTeamOpConnectionDeleteRequest) -> ConnectionDeleteRequest:
        return ConnectionDeleteRequest(
            workbook_id=op_rq.workbook_id,
            name=op_rq.name,
        )

    @translate_op_rs.register
    def translate_op_rs_connection_delete(self, op_rs: ConnectionDeleteResponse) -> YaTeamOpConnectionDeleteResponse:
        return YaTeamOpConnectionDeleteResponse()

    # Advise dataset fields
    @translate_op_rq.register
    def translate_op_rq_advise_dataset_fields(
            self,
            op_rq: YaTeamOpAdviseDatasetFieldsRequest
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
    ) -> YaTeamOpAdviseDatasetFieldsResponse:
        return YaTeamOpAdviseDatasetFieldsResponse(
            dataset=op_rs.dataset,
        )
