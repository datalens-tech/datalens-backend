import enum
from typing import ClassVar, Sequence, Optional

import attr

from bi_external_api.attrs_model_mapper import ModelDescriptor, AttribDescriptor
from bi_external_api.attrs_model_mapper.utils import MText
from bi_external_api.domain.utils import ensure_tuple


class EntryKind(enum.Enum):
    connection = enum.auto()
    dataset = enum.auto()
    chart = enum.auto()
    dashboard = enum.auto()


class EntryRefKind(enum.Enum):
    wb_ref = enum.auto()
    id_ref = enum.auto()


class SecretKind(enum.Enum):
    plain = enum.auto()


@ModelDescriptor(is_abstract=True, children_type_discriminator_attr_name="kind")
@attr.s(frozen=True)
class EntryRef:
    kind: ClassVar[EntryRefKind]


@ModelDescriptor()
@attr.s(frozen=True)
class EntryIDRef(EntryRef):
    kind = EntryRefKind.id_ref

    id: str = attr.ib()


@ModelDescriptor()
@attr.s(frozen=True, kw_only=True)
class EntryWBRef(EntryRef):
    kind = EntryRefKind.wb_ref

    wb_id: str = attr.ib()
    entry_name: str = attr.ib()


@ModelDescriptor()
@attr.s(frozen=True)
class EntryInfo:
    kind: EntryKind = attr.ib()
    name: str = attr.ib()
    id: str = attr.ib()


@ModelDescriptor(is_abstract=True, children_type_discriminator_attr_name="kind")
@attr.s(frozen=True)
class Secret:
    kind: ClassVar[SecretKind]


@ModelDescriptor()
@attr.s(frozen=True)
class PlainSecret(Secret):
    kind = SecretKind.plain

    secret: str = attr.ib(repr=False, metadata=AttribDescriptor(load_only=True).to_meta())


@ModelDescriptor()
@attr.s(frozen=True, kw_only=True)
class NameMapEntry:
    local_name: str = attr.ib(metadata=AttribDescriptor(
        description=MText(
            ru="Имя сущности в рамках воркбука",
            en="Entity name within a workbook",
        )
    ).to_meta()
    )
    entry_kind: EntryKind = attr.ib()
    unique_entry_id: Optional[str] = attr.ib(default=None)
    legacy_location: Sequence[str] = attr.ib(  # type: ignore
        converter=ensure_tuple,
        metadata=AttribDescriptor(
            description=MText(
                ru="Путь до сущности в навигации",
                en="Path to entity in navigation",
            )
        ).to_meta()
    )
