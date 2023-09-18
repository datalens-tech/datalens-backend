from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    List,
    Optional,
    Sequence,
    Set,
)

import attr

from dl_formula.core.dialect import DialectCombo
from dl_formula_ref.registry.aliased_res import AliasedResourceRegistry
from dl_formula_ref.registry.arg_extractor import DefaultArgumentExtractor
from dl_formula_ref.registry.dialect_extractor import DefaultDialectExtractor
from dl_formula_ref.registry.impl_selector import DefaultImplementationSelector
from dl_formula_ref.registry.naming import DefaultNamingProvider
from dl_formula_ref.registry.note_extractor import DefaultNoteExtractor
from dl_formula_ref.registry.return_extractor import DefaultReturnTypeExtractor
from dl_formula_ref.registry.scopes import SCOPES_DEFAULT
from dl_formula_ref.registry.signature_gen import DefaultSignatureGenerator
from dl_formula_ref.registry.text import ParameterizedText

if TYPE_CHECKING:
    import dl_formula_ref.registry.arg_base as _arg_base
    from dl_formula_ref.registry.dialect_base import DialectExtractorBase
    from dl_formula_ref.registry.env import GenerationEnvironment
    from dl_formula_ref.registry.example_base import ExampleBase
    from dl_formula_ref.registry.impl_selector_base import ImplementationSelectorBase
    from dl_formula_ref.registry.impl_spec import FunctionImplementationSpec
    from dl_formula_ref.registry.naming import NamingProviderBase
    from dl_formula_ref.registry.note import (
        Note,
        ParameterizedNote,
    )
    import dl_formula_ref.registry.note_extr_base as _note_extr_base
    import dl_formula_ref.registry.return_base as _return_base
    import dl_formula_ref.registry.signature_base as _signature_base


@attr.s(frozen=True)
class FunctionDocCategory:
    _name: str = attr.ib(kw_only=True)
    _description: str = attr.ib(kw_only=True)
    _keywords: str = attr.ib(kw_only=True)
    _resources: AliasedResourceRegistry = attr.ib(kw_only=True, factory=AliasedResourceRegistry)

    @property
    def name(self) -> str:
        return self._name

    @property
    def description(self) -> str:
        return self._description

    @property
    def keywords(self) -> str:
        return self._keywords

    @property
    def resources(self) -> AliasedResourceRegistry:
        return self._resources


@attr.s(frozen=True)
class FunctionDocRegistryItem:
    _category: FunctionDocCategory = attr.ib(kw_only=True)
    _name: str = attr.ib(kw_only=True)
    _is_window: bool = attr.ib(kw_only=True, default=False)
    _description: str = attr.ib(kw_only=True)
    _scopes: int = attr.ib(kw_only=True, default=SCOPES_DEFAULT)
    _notes: List[Note] = attr.ib(kw_only=True, factory=list)
    _examples: Sequence[ExampleBase] = attr.ib(kw_only=True, factory=list)
    _impl_selector: ImplementationSelectorBase = attr.ib(kw_only=True, factory=DefaultImplementationSelector)
    _signature_gen: _signature_base.SignatureGeneratorBase = attr.ib(kw_only=True, factory=DefaultSignatureGenerator)
    _resources: AliasedResourceRegistry = attr.ib(kw_only=True, factory=AliasedResourceRegistry)
    _arg_extractor: _arg_base.ArgumentExtractorBase = attr.ib(kw_only=True, factory=DefaultArgumentExtractor)
    _note_extractor: _note_extr_base.NoteExtractorBase = attr.ib(kw_only=True, factory=DefaultNoteExtractor)
    _return_type_extractor: _return_base.ReturnTypeExtractorBase = attr.ib(
        kw_only=True, factory=DefaultReturnTypeExtractor
    )
    _dialect_extractor: DialectExtractorBase = attr.ib(kw_only=True, factory=DefaultDialectExtractor)
    _naming_provider: NamingProviderBase = attr.ib(kw_only=True, factory=DefaultNamingProvider)

    @property
    def category(self) -> FunctionDocCategory:
        return self._category

    @property
    def category_name(self) -> str:
        return self._category.name

    @property
    def name(self) -> str:
        return self._name

    @property
    def is_window(self) -> bool:
        return self._is_window

    @property
    def internal_name(self) -> str:
        return self._naming_provider.get_internal_name(self)

    def get_title(self, locale: str) -> str:
        return self._naming_provider.get_title(self, locale=locale)

    def get_short_title(self, locale: str) -> str:
        return self._naming_provider.get_short_title(self, locale=locale)

    @property
    def description(self) -> str:
        return self._description

    @property
    def resources(self) -> AliasedResourceRegistry:
        return self._resources

    @property
    def all_resources(self) -> AliasedResourceRegistry:
        return self._category.resources + self._resources

    def get_explicit_notes(self, env: GenerationEnvironment) -> List[Note]:
        return [note for note in self._notes if note.scopes & env.scopes == env.scopes]

    def get_examples(self, env: GenerationEnvironment) -> Sequence[ExampleBase]:
        return [example for example in self._examples if example.scopes & env.scopes == env.scopes]

    def get_notes(self, env: GenerationEnvironment) -> List[ParameterizedNote]:
        return self._note_extractor.get_notes(self, env=env)

    def get_implementation_specs(self, env: GenerationEnvironment) -> List[FunctionImplementationSpec]:
        return self._impl_selector.get_implementations(item=self, env=env)

    def get_signatures(self, env: GenerationEnvironment) -> _signature_base.FunctionSignatureCollection:
        return self._signature_gen.get_signatures(item=self, env=env)

    def get_return_type(self, env: GenerationEnvironment) -> ParameterizedText:
        return self._return_type_extractor.get_return_type(item=self, env=env)

    def get_dialects(self, env: GenerationEnvironment) -> Set[DialectCombo]:
        return self._dialect_extractor.get_dialects(item=self, env=env)

    def _get_one_implementation_spec(self, env: GenerationEnvironment) -> Optional[FunctionImplementationSpec]:
        implementations = self.get_implementation_specs(env=env)
        if implementations:
            return implementations[0]
        return None

    def is_supported(self, env: GenerationEnvironment) -> bool:
        # Validate against own scopes
        if self._scopes & env.scopes != env.scopes:
            return False

        # Filter implementations by scopes
        impl = self._get_one_implementation_spec(env=env)
        scopes: int = 0
        if impl is not None:
            scopes = impl.scopes
        return scopes & env.scopes == env.scopes

    def get_args(self, env: GenerationEnvironment) -> List[_arg_base.FuncArg]:
        return self._arg_extractor.get_args(self, env=env)
