import functools
from typing import ClassVar, Optional

import attr

from bi_external_api.attrs_model_mapper import ModelDescriptor
from bi_external_api.enums import ExtAPIType
from .common import EntryInfo, Secret, EntryWBRef
from .dataset_main import Dataset
from .object_model import ObjectParent
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
)
from .workbook import WorkBook, ConnectionInstance

UNIFIED_API_TYPES = [ExtAPIType.UNIFIED_DC, ExtAPIType.UNIFIED_NEBIUS_IL]


@ModelDescriptor(api_types=UNIFIED_API_TYPES, is_abstract=True, children_type_discriminator_attr_name="kind")
@attr.s(frozen=True)
class UnifiedV0OpRequest:
    kind: ClassVar[WorkbookOpKind]

    @classmethod
    def get_operation_kind(cls) -> WorkbookOpKind:
        return cls.kind

    def get_target_workbook_id(self) -> Optional[str]:
        return None

    def get_target_parent_object(self) -> Optional[ObjectParent]:
        return None


@ModelDescriptor(api_types=UNIFIED_API_TYPES, is_abstract=True, children_type_discriminator_attr_name="kind")
@attr.s(frozen=True)
class UnifiedV0OpResponse:
    kind: ClassVar[WorkbookOpKind]


class UnifiedV0CreateWorkbookRequest():
    pass


#
# Workbook read
#
@ModelDescriptor(api_types=UNIFIED_API_TYPES)
@attr.s(frozen=True)
class UnifiedV0OpWorkbookGetRequest(UnifiedV0OpRequest):
    kind = WorkbookOpKind.wb_read

    workbook_id: str = attr.ib()

    def get_target_workbook_id(self) -> Optional[str]:
        return self.workbook_id


@ModelDescriptor(api_types=UNIFIED_API_TYPES)
@attr.s(frozen=True)
class UnifiedV0OpWorkbookGetResponse(UnifiedV0OpResponse):
    kind = WorkbookOpKind.wb_read

    workbook: WorkBook = attr.ib()


#
# Workbook create
#
@ModelDescriptor(api_types=UNIFIED_API_TYPES)
@attr.s(frozen=True)
class UnifiedV0OpWorkbookCreateRequest(UnifiedV0OpRequest):
    kind = WorkbookOpKind.wb_create

    parent: ObjectParent = attr.ib()
    workbook_title: str = attr.ib()

    def get_target_parent_object(self) -> Optional[ObjectParent]:
        return self.parent


@ModelDescriptor(api_types=UNIFIED_API_TYPES)
@attr.s(frozen=True)
class UnifiedV0OpWorkbookCreateResponse(UnifiedV0OpResponse):
    kind = WorkbookOpKind.wb_create

    workbook_id: str = attr.ib()

    def get_target_workbook_id(self) -> Optional[str]:
        return self.workbook_id


#
# Workbook modify
#
@ModelDescriptor(api_types=UNIFIED_API_TYPES)
@attr.s(frozen=True)
class UnifiedV0OpWorkbookModifyRequest(UnifiedV0OpRequest):
    kind = WorkbookOpKind.wb_modify

    workbook_id: str = attr.ib()
    workbook: WorkBook = attr.ib()
    force_rewrite: Optional[bool] = attr.ib(default=None)

    def get_target_workbook_id(self) -> Optional[str]:
        return self.workbook_id


@ModelDescriptor(api_types=UNIFIED_API_TYPES)
@attr.s(frozen=True)
class UnifiedV0OpWorkbookModifyResponse(UnifiedV0OpResponse):
    kind = WorkbookOpKind.wb_modify

    executed_plan: ModificationPlan = attr.ib()
    workbook: WorkBook = attr.ib()


#
# Workbook delete
#
@ModelDescriptor(api_types=UNIFIED_API_TYPES)
@attr.s(frozen=True)
class UnifiedV0OpWorkbookDeleteRequest(UnifiedV0OpRequest):
    kind = WorkbookOpKind.wb_delete

    workbook_id: str = attr.ib()

    def get_target_workbook_id(self) -> Optional[str]:
        return self.workbook_id


@ModelDescriptor(api_types=UNIFIED_API_TYPES)
@attr.s(frozen=True)
class UnifiedV0OpWorkbookDeleteResponse(UnifiedV0OpResponse):
    kind = WorkbookOpKind.wb_delete


#
# Connection get
#
@ModelDescriptor(api_types=UNIFIED_API_TYPES)
@attr.s(frozen=True)
class UnifiedV0OpConnectionGetRequest(UnifiedV0OpRequest):
    kind = WorkbookOpKind.connection_get

    workbook_id: str = attr.ib()
    name: str = attr.ib()

    def get_target_workbook_id(self) -> Optional[str]:
        return self.workbook_id


@ModelDescriptor(api_types=UNIFIED_API_TYPES)
@attr.s(frozen=True)
class UnifiedV0OpConnectionGetResponse(UnifiedV0OpResponse):
    kind = WorkbookOpKind.connection_get

    connection: ConnectionInstance = attr.ib()


#
# Connection create
#
@ModelDescriptor(api_types=UNIFIED_API_TYPES)
@attr.s(frozen=True)
class UnifiedV0OpConnectionCreateRequest(UnifiedV0OpRequest):
    kind = WorkbookOpKind.connection_create

    workbook_id: str = attr.ib()
    connection: ConnectionInstance = attr.ib()
    secret: Optional[Secret] = attr.ib()

    def get_target_workbook_id(self) -> Optional[str]:
        return self.workbook_id


@ModelDescriptor(api_types=UNIFIED_API_TYPES)
@attr.s(frozen=True)
class UnifiedV0OpConnectionCreateResponse(UnifiedV0OpResponse):
    kind = WorkbookOpKind.connection_create

    connection_info: EntryInfo = attr.ib()


#
# Connection modify
#
@ModelDescriptor(api_types=UNIFIED_API_TYPES)
@attr.s(frozen=True)
class UnifiedV0OpConnectionModifyRequest(UnifiedV0OpRequest):
    kind = WorkbookOpKind.connection_modify

    workbook_id: str = attr.ib()
    connection: ConnectionInstance = attr.ib()
    secret: Optional[Secret] = attr.ib()

    def get_target_workbook_id(self) -> Optional[str]:
        return self.workbook_id


@ModelDescriptor(api_types=UNIFIED_API_TYPES)
@attr.s(frozen=True)
class UnifiedV0OpConnectionModifyResponse(UnifiedV0OpResponse):
    kind = WorkbookOpKind.connection_modify


#
# Connection delete
#
@ModelDescriptor(api_types=UNIFIED_API_TYPES)
@attr.s(frozen=True)
class UnifiedV0OpConnectionDeleteRequest(UnifiedV0OpRequest):
    kind = WorkbookOpKind.connection_delete

    workbook_id: str = attr.ib()
    name: str = attr.ib()

    def get_target_workbook_id(self) -> Optional[str]:
        return self.workbook_id


@ModelDescriptor(api_types=UNIFIED_API_TYPES)
@attr.s(frozen=True)
class UnifiedV0OpConnectionDeleteResponse(UnifiedV0OpResponse):
    kind = WorkbookOpKind.connection_delete


#
# Advise dataset fields
#
@ModelDescriptor(api_types=UNIFIED_API_TYPES)
@attr.s(frozen=True)
class UnifiedV0OpAdviseDatasetFieldsRequest(UnifiedV0OpRequest):
    kind = WorkbookOpKind.advise_dataset_fields

    workbook_id: str = attr.ib()
    connection_name: str = attr.ib()
    partial_dataset: Dataset = attr.ib()

    def get_target_workbook_id(self) -> Optional[str]:
        return self.workbook_id


@ModelDescriptor(api_types=UNIFIED_API_TYPES)
@attr.s(frozen=True)
class UnifiedV0OpAdviseDatasetFieldsResponse(UnifiedV0OpResponse):
    kind = WorkbookOpKind.advise_dataset_fields

    dataset: Dataset = attr.ib()


#
# Translator
#
class UnifiedV0APIOperationTranslator(
    ParticularAPIOperationTranslator[UnifiedV0OpRequest, UnifiedV0OpResponse]
):
    @functools.singledispatchmethod
    def translate_op_rq(self, op_rq: UnifiedV0OpRequest) -> WorkbookOpRequest:
        raise NotImplementedError()

    # TODO FIX: Try to remove (type: ignore) after MyPy upgrade
    @functools.singledispatchmethod
    def translate_op_rs(self, op_rs: WorkbookOpResponse) -> UnifiedV0OpResponse:  # type: ignore
        raise NotImplementedError()

    # Read
    @translate_op_rq.register
    def translate_op_rq_wb_get(self, op_rq: UnifiedV0OpWorkbookGetRequest) -> WorkbookReadRequest:
        return WorkbookReadRequest(
            workbook_id=op_rq.workbook_id,
            use_id_formula=True,
        )

    @translate_op_rs.register
    def translate_op_rs_wb_get(self, op_rs: WorkbookReadResponse) -> UnifiedV0OpWorkbookGetResponse:
        return UnifiedV0OpWorkbookGetResponse(
            workbook=op_rs.workbook,
        )

    # Create
    @translate_op_rq.register
    def translate_op_rq_wb_create(self, op_rq: UnifiedV0OpWorkbookCreateRequest) -> TrueWorkbookCreateRequest:
        return TrueWorkbookCreateRequest(
            workbook_title=op_rq.workbook_title,
        )

    @translate_op_rs.register
    def translate_op_rs_wb_create(self, op_rs: TrueWorkbookCreateResponse) -> UnifiedV0OpWorkbookCreateResponse:
        return UnifiedV0OpWorkbookCreateResponse(
            workbook_id=op_rs.workbook_id,
        )

    # Modify
    @translate_op_rq.register
    def translate_op_rq_wb_modify(self, op_rq: UnifiedV0OpWorkbookModifyRequest) -> WorkbookWriteRequest:
        return WorkbookWriteRequest(
            workbook_id=op_rq.workbook_id,
            workbook=op_rq.workbook,
            force_rewrite=op_rq.force_rewrite,
        )

    @translate_op_rs.register
    def translate_op_rs_wb_modify(self, op_rs: WorkbookWriteResponse) -> UnifiedV0OpWorkbookModifyResponse:
        return UnifiedV0OpWorkbookModifyResponse(
            executed_plan=op_rs.executed_plan,
            workbook=op_rs.workbook,
        )

    # Delete
    @translate_op_rq.register
    def translate_op_rq_wb_delete(self, op_rq: UnifiedV0OpWorkbookDeleteRequest) -> WorkbookDeleteRequest:
        return WorkbookDeleteRequest(
            workbook_id=op_rq.workbook_id,
        )

    @translate_op_rs.register
    def translate_op_rs_wb_delete(self, op_rs: WorkbookDeleteResponse) -> UnifiedV0OpWorkbookDeleteResponse:
        return UnifiedV0OpWorkbookDeleteResponse(
        )

    # Connection get
    @translate_op_rq.register
    def translate_op_rq_connection_get(self, op_rq: UnifiedV0OpConnectionGetRequest) -> ConnectionGetRequest:
        return ConnectionGetRequest(
            workbook_id=op_rq.workbook_id,
            name=op_rq.name,
        )

    @translate_op_rs.register
    def translate_op_rs_connection_get(self, op_rs: ConnectionGetResponse) -> UnifiedV0OpConnectionGetResponse:
        return UnifiedV0OpConnectionGetResponse(
            connection=op_rs.connection,
        )

    # Connection create
    @translate_op_rq.register
    def translate_op_rq_connection_create(self, op_rq: UnifiedV0OpConnectionCreateRequest) -> ConnectionCreateRequest:
        return ConnectionCreateRequest(
            workbook_id=op_rq.workbook_id,
            connection=op_rq.connection,
            secret=op_rq.secret,
        )

    @translate_op_rs.register
    def translate_op_rs_connection_create(self, op_rs: ConnectionCreateResponse) -> UnifiedV0OpConnectionCreateResponse:
        return UnifiedV0OpConnectionCreateResponse(
            connection_info=op_rs.connection_info,
        )

    # Connection modify
    @translate_op_rq.register
    def translate_op_rq_connection_modify(self, op_rq: UnifiedV0OpConnectionModifyRequest) -> ConnectionModifyRequest:
        return ConnectionModifyRequest(
            workbook_id=op_rq.workbook_id,
            connection=op_rq.connection,
            secret=op_rq.secret,
        )

    @translate_op_rs.register
    def translate_op_rs_connection_modify(self, op_rs: ConnectionModifyResponse) -> UnifiedV0OpConnectionModifyResponse:
        return UnifiedV0OpConnectionModifyResponse(
        )

    # Connection delete
    @translate_op_rq.register
    def translate_op_rq_connection_delete(self, op_rq: UnifiedV0OpConnectionDeleteRequest) -> ConnectionDeleteRequest:
        return ConnectionDeleteRequest(
            workbook_id=op_rq.workbook_id,
            name=op_rq.name,
        )

    @translate_op_rs.register
    def translate_op_rs_connection_delete(self, op_rs: ConnectionDeleteResponse) -> UnifiedV0OpConnectionDeleteResponse:
        return UnifiedV0OpConnectionDeleteResponse()

    # Advise dataset fields
    @translate_op_rq.register
    def translate_op_rq_advise_dataset_fields(
            self,
            op_rq: UnifiedV0OpAdviseDatasetFieldsRequest
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
    ) -> UnifiedV0OpAdviseDatasetFieldsResponse:
        return UnifiedV0OpAdviseDatasetFieldsResponse(
            dataset=op_rs.dataset,
        )
