import abc
import enum
from typing import (
    ClassVar,
    Generic,
    Optional,
    Sequence,
    TypeVar,
)

import attr

from bi_external_api.attrs_model_mapper import (
    AttribDescriptor,
    ModelDescriptor,
)
from bi_external_api.attrs_model_mapper.utils import MText
from bi_external_api.domain.utils import ensure_tuple

from . import EntryKind
from ...enums import ExtAPIType
from .common import (
    EntryInfo,
    EntryRef,
    NameMapEntry,
    Secret,
)
from .connection import ConnectionSecret
from .dataset_main import Dataset
from .workbook import (
    ConnectionInstance,
    WorkBook,
    WorkbookConnectionsOnly,
    WorkbookIndexItem,
)


class WorkbookOpKind(enum.Enum):
    wb_create = enum.auto()
    wb_modify = enum.auto()
    wb_read = enum.auto()
    wb_delete = enum.auto()
    wb_clusterize = enum.auto()
    wb_list = enum.auto()
    connection_create = enum.auto()
    connection_modify = enum.auto()
    connection_get = enum.auto()
    connection_delete = enum.auto()
    advise_dataset_fields = enum.auto()


class EntryOperationKind(enum.Enum):
    create = enum.auto()
    modify = enum.auto()
    delete = enum.auto()


@ModelDescriptor()
@attr.s(frozen=True)
class EntryOperation:
    entry_name: str = attr.ib()
    entry_kind: EntryKind = attr.ib()
    operation_kind: EntryOperationKind = attr.ib()


@ModelDescriptor()
@attr.s(frozen=True)
class ModificationPlan:
    operations: Sequence[EntryOperation] = attr.ib(converter=ensure_tuple)


#
# Base
#
@ModelDescriptor(is_abstract=True, children_type_discriminator_attr_name="kind")
@attr.s(frozen=True)
class WorkbookOpRequest:
    kind: ClassVar[WorkbookOpKind]

    @classmethod
    def get_operation_kind(cls) -> WorkbookOpKind:
        return cls.kind


@ModelDescriptor(is_abstract=True, children_type_discriminator_attr_name="kind")
@attr.s(frozen=True)
class WorkbookOpResponse:
    kind: ClassVar[WorkbookOpKind]


#
# Workbook Create
#
@ModelDescriptor(api_types_exclude=[ExtAPIType.DC])
@attr.s(frozen=True)
class FakeWorkbookCreateRequest(WorkbookOpRequest):
    kind = WorkbookOpKind.wb_create

    workbook_id: str = attr.ib()
    workbook: Optional[WorkbookConnectionsOnly] = attr.ib(default=None)
    connection_secrets: Sequence[ConnectionSecret] = attr.ib(
        converter=ensure_tuple,
        factory=tuple,
        metadata=AttribDescriptor(
            description=MText(
                ru="Секреты, используемые в подключениях (пароли/токены)."
                " Соответствие секрета подключению определяется по имени."
                " Поле не требуется для подключений без секретов,"
                " например, для CHYT с пользовательской аутентификацией.",
                en="Secrets used in connections (passwords/tokens)."
                " Сorrespondence of the secret to the connection is determined by the name."
                " This field is not required for connections that don't use secrets,"
                " for example, for CHYT with user authentication.",
            )
        ).to_meta(),
    )


@ModelDescriptor(api_types_exclude=[ExtAPIType.DC])
@attr.s(frozen=True)
class FakeWorkbookCreateResponse(WorkbookOpResponse):
    kind = WorkbookOpKind.wb_create

    workbook_id: str = attr.ib()
    created_entries_info: Sequence[EntryInfo] = attr.ib()


@ModelDescriptor(api_types=[ExtAPIType.DC])
@attr.s(frozen=True)
class TrueWorkbookCreateRequest(WorkbookOpRequest):
    kind = WorkbookOpKind.wb_create

    workbook_title: str = attr.ib()


@ModelDescriptor(api_types=[ExtAPIType.DC])
@attr.s(frozen=True)
class TrueWorkbookCreateResponse(WorkbookOpResponse):
    kind = WorkbookOpKind.wb_create

    workbook_id: str = attr.ib()


#
# Workbook Modify
#
@ModelDescriptor()
@attr.s(frozen=True)
class WorkbookWriteRequest(WorkbookOpRequest):
    kind = WorkbookOpKind.wb_modify

    workbook_id: str = attr.ib()
    workbook: WorkBook = attr.ib()
    force_rewrite: Optional[bool] = attr.ib(default=None)
    # Here will be also connection secrets


@ModelDescriptor()
@attr.s(frozen=True)
class WorkbookWriteResponse(WorkbookOpResponse):
    kind = WorkbookOpKind.wb_modify

    executed_plan: ModificationPlan = attr.ib()
    workbook: WorkBook = attr.ib()


#
# Workbook Delete
#
@ModelDescriptor(api_types=[ExtAPIType.DC])
@attr.s(frozen=True)
class WorkbookDeleteRequest(WorkbookOpRequest):
    kind = WorkbookOpKind.wb_delete

    workbook_id: str = attr.ib()


@ModelDescriptor(api_types=[ExtAPIType.DC])
@attr.s(frozen=True)
class WorkbookDeleteResponse(WorkbookOpResponse):
    kind = WorkbookOpKind.wb_delete


#
# Workbook Read
#
@ModelDescriptor()
@attr.s(frozen=True)
class WorkbookReadRequest(WorkbookOpRequest):
    kind = WorkbookOpKind.wb_read

    workbook_id: str = attr.ib()
    use_id_formula: Optional[bool] = attr.ib(default=False)


@ModelDescriptor()
@attr.s(frozen=True)
class WorkbookReadResponse(WorkbookOpResponse):
    kind = WorkbookOpKind.wb_read

    workbook: WorkBook = attr.ib()
    id: Optional[str] = attr.ib(default=None)
    title: Optional[str] = attr.ib(default=None)
    project_id: Optional[str] = attr.ib(default=None)


#
# Connection get
#
@ModelDescriptor(api_types=[ExtAPIType.DC])
@attr.s(frozen=True)
class ConnectionGetRequest(WorkbookOpRequest):
    kind = WorkbookOpKind.connection_get

    workbook_id: str = attr.ib()
    name: str = attr.ib()


@ModelDescriptor(api_types=[ExtAPIType.DC])
@attr.s(frozen=True)
class ConnectionGetResponse(WorkbookOpResponse):
    kind = WorkbookOpKind.connection_get

    connection: ConnectionInstance = attr.ib()


#
# Connection create
#
@ModelDescriptor()
@attr.s(frozen=True)
class ConnectionCreateRequest(WorkbookOpRequest):
    kind = WorkbookOpKind.connection_create

    workbook_id: str = attr.ib()
    connection: ConnectionInstance = attr.ib()
    secret: Optional[Secret] = attr.ib()


@ModelDescriptor()
@attr.s(frozen=True)
class ConnectionCreateResponse(WorkbookOpResponse):
    kind = WorkbookOpKind.connection_create

    connection_info: EntryInfo = attr.ib()


#
# Connection modify
#
@ModelDescriptor(api_types=[ExtAPIType.DC])
@attr.s(frozen=True)
class ConnectionModifyRequest(WorkbookOpRequest):
    kind = WorkbookOpKind.connection_modify

    workbook_id: str = attr.ib()
    connection: ConnectionInstance = attr.ib()
    secret: Optional[Secret] = attr.ib()


@ModelDescriptor(api_types=[ExtAPIType.DC])
@attr.s(frozen=True)
class ConnectionModifyResponse(WorkbookOpResponse):
    kind = WorkbookOpKind.connection_modify


#
# Connection delete
#
@ModelDescriptor(api_types=[ExtAPIType.DC])
@attr.s(frozen=True)
class ConnectionDeleteRequest(WorkbookOpRequest):
    kind = WorkbookOpKind.connection_delete

    workbook_id: str = attr.ib()
    name: str = attr.ib()


@ModelDescriptor(api_types=[ExtAPIType.DC])
@attr.s(frozen=True)
class ConnectionDeleteResponse(WorkbookOpResponse):
    kind = WorkbookOpKind.connection_delete


#
# Advise dataset fields
#
@ModelDescriptor()
@attr.s(frozen=True)
class AdviseDatasetFieldsRequest(WorkbookOpRequest):
    kind = WorkbookOpKind.advise_dataset_fields

    connection_ref: EntryRef = attr.ib()
    partial_dataset: Dataset = attr.ib()

    use_id_formula: Optional[bool] = attr.ib(default=False)


@ModelDescriptor()
@attr.s(frozen=True)
class AdviseDatasetFieldsResponse(WorkbookOpResponse):
    kind = WorkbookOpKind.advise_dataset_fields

    dataset: Dataset = attr.ib()


@ModelDescriptor()
@attr.s(frozen=True)
class WorkbookClusterizeRequest(WorkbookOpRequest):
    kind = WorkbookOpKind.wb_clusterize

    dash_id_list: Sequence[str] = attr.ib(default=())
    navigation_folder_path: Optional[str] = attr.ib(default=None)


@ModelDescriptor()
@attr.s(frozen=True)
class WorkbookClusterizeResponse(WorkbookOpResponse):
    kind = WorkbookOpKind.wb_clusterize

    workbook: WorkBook = attr.ib()
    name_map: Sequence[NameMapEntry] = attr.ib(converter=ensure_tuple)


@ModelDescriptor()
@attr.s(frozen=True)
class WorkbookListRequest(WorkbookOpRequest):
    kind = WorkbookOpKind.wb_list

    project_id: str = attr.ib()


@ModelDescriptor()
@attr.s(frozen=True)
class WorkbookListResponse(WorkbookOpResponse):
    kind = WorkbookOpKind.wb_list

    workbooks: Sequence[WorkbookIndexItem] = attr.ib()


#
# Translator
#
_API_BASE_RQ_TV = TypeVar("_API_BASE_RQ_TV")
_API_BASE_RS_TV = TypeVar("_API_BASE_RS_TV")


class ParticularAPIOperationTranslator(Generic[_API_BASE_RQ_TV, _API_BASE_RS_TV], metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def translate_op_rq(self, op_rq: _API_BASE_RQ_TV) -> WorkbookOpRequest:
        raise NotImplementedError()

    @abc.abstractmethod
    def translate_op_rs(self, op_rs: WorkbookOpResponse) -> _API_BASE_RS_TV:
        raise NotImplementedError()
