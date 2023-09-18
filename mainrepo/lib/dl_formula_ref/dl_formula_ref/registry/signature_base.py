from __future__ import annotations

import abc
from enum import (
    Enum,
    unique,
)
from typing import (
    TYPE_CHECKING,
    List,
)

import attr

if TYPE_CHECKING:
    import dl_formula_ref.registry.base as _registry_base
    from dl_formula_ref.registry.env import GenerationEnvironment


@attr.s(frozen=True)
class FunctionSignature:
    title: str = attr.ib(kw_only=True, default="")
    body: str = attr.ib(kw_only=True)
    description: list[str] = attr.ib(kw_only=True, factory=list)


@unique
class SignaturePlacement(Enum):
    compact = "compact"
    tabbed = "tabbed"


@attr.s(frozen=True)
class FunctionSignatureCollection:
    signatures: List[FunctionSignature] = attr.ib(kw_only=True)
    placement_mode: SignaturePlacement = attr.ib(kw_only=True, default=SignaturePlacement.compact)


@attr.s(frozen=True)
class SignatureGeneratorBase(abc.ABC):
    @abc.abstractmethod
    def get_signatures(
        self,
        item: _registry_base.FunctionDocRegistryItem,
        env: GenerationEnvironment,
    ) -> FunctionSignatureCollection:
        raise NotImplementedError
