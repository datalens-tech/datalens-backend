from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    List,
    Sequence,
)

import attr

from dl_formula.inspect.function import (
    requires_grouping,
    supports_bfb,
    supports_extensions,
    supports_grouping,
    supports_ignore_dimensions,
    supports_lod,
    supports_ordering,
)
from dl_formula_ref.registry.signature_base import (
    FunctionSignature,
    FunctionSignatureCollection,
    SignatureGeneratorBase,
    SignaturePlacement,
)
from dl_formula_ref.texts import (
    SIGNATURE_DESC_EXTENDED_HEADER,
    SIGNATURE_TITLE_EXTENDED,
    SIGNATURE_TITLE_STANDARD,
)

if TYPE_CHECKING:
    import dl_formula_ref.registry.arg_base as _arg_base
    import dl_formula_ref.registry.base as _registry_base
    from dl_formula_ref.registry.env import GenerationEnvironment


@attr.s(frozen=True)
class StaticSignatureGenerator(SignatureGeneratorBase):
    signatures: List[FunctionSignature] = attr.ib(kw_only=True)

    def get_signatures(
        self,
        item: _registry_base.FunctionDocRegistryItem,
        env: GenerationEnvironment,
    ) -> FunctionSignatureCollection:
        return FunctionSignatureCollection(
            signatures=self.signatures,
        )


@attr.s(frozen=True)
class SignatureTemplate:
    title: str = attr.ib(kw_only=True, default="")
    body: str = attr.ib(kw_only=True)


@attr.s(frozen=True)
class TemplatedSignatureGenerator(SignatureGeneratorBase):
    """
    Generates signatures from templates and the names of the function's arguments
    """

    _signature_templates: Sequence[SignatureTemplate] = attr.ib(kw_only=True)
    _placement_mode: SignaturePlacement = attr.ib(kw_only=True, default=SignaturePlacement.compact)

    def get_signatures(
        self,
        item: _registry_base.FunctionDocRegistryItem,
        env: GenerationEnvironment,
    ) -> FunctionSignatureCollection:
        func_name = item.name.upper()
        args = item.get_args(env=env)
        signatures = [
            FunctionSignature(
                title=template.title,
                body=template.body.strip().format(func_name, *(arg.name for arg in args)),
            )
            for template in self._signature_templates
        ]
        sig_collection = FunctionSignatureCollection(placement_mode=self._placement_mode, signatures=signatures)
        return sig_collection


@attr.s(frozen=True)
class DefaultSignatureGenerator(SignatureGeneratorBase):
    """
    Generates signatures from the properties of the function and its argument names
    """

    _placement_mode: SignaturePlacement = attr.ib(kw_only=True, default=SignaturePlacement.tabbed)

    def _get_signature_from_args(
        self,
        func_name: str,
        args: List[_arg_base.FuncArg],
        inf_args: bool,
        is_window: bool,
        is_extended_syntax: bool,
        category_name: str,
    ) -> FunctionSignature:
        if not args:  # to avoid extra spaces with 0-argument functions
            return FunctionSignature(body="{}()".format(func_name))

        args_str = ""
        cur_opt_level = 0
        for i, arg in enumerate(args):
            if arg.optional_level > cur_opt_level:
                args_str += " [ " * (arg.optional_level - cur_opt_level)
            elif arg.optional_level < cur_opt_level:
                args_str += " ] " * (cur_opt_level - arg.optional_level)
            cur_opt_level = arg.optional_level
            if i > 0:
                args_str += ", "
            args_str += arg.name
        if inf_args:
            args_str += " [ , ... ]"
        if cur_opt_level > 0:
            args_str += " ]" * cur_opt_level

        modifiers: List[str] = []
        description: list[str] = []
        if requires_grouping(func_name, is_window=is_window):
            modifiers.append("TOTAL | WITHIN ... | AMONG ...")
            description.append(f"- {{category:{category_name}#syntax-grouping:TOTAL, WITHIN, AMONG}}")
        title: str
        if is_extended_syntax:
            title = SIGNATURE_TITLE_EXTENDED
            if supports_lod(func_name, is_window=is_window):
                modifiers.append("[ FIXED ... | INCLUDE ... | EXCLUDE ... ]")
                description.append(f"- {{category:{category_name}#syntax-lod:FIXED, INCLUDE, EXCLUDE}}")
            if supports_grouping(func_name, is_window=is_window) and not requires_grouping(
                func_name, is_window=is_window
            ):
                # If grouping is required (e.g. window SUM),
                # its non-optional form has already been added to modifiers
                modifiers.append("[ TOTAL | WITHIN ... | AMONG ... ]")
                description.append(f"- {{category:{category_name}#syntax-grouping:TOTAL, WITHIN, AMONG}}")
            if supports_ordering(func_name, is_window=is_window):
                modifiers.append("[ ORDER BY ... ]")
                description.append(f"- {{category:{category_name}#syntax-order-by:ORDER BY}}")
            if supports_bfb(func_name, is_window=is_window):
                modifiers.append("[ BEFORE FILTER BY ... ]")
                description.append(f"- {{category:{category_name}#syntax-before-filter-by:BEFORE FILTER BY}}")
            if supports_ignore_dimensions(func_name, is_window=is_window):
                modifiers.append("[ IGNORE DIMENSIONS ... ]")
                description.append(f"- {{category:{category_name}#syntax-ignore-dimensions:IGNORE DIMENSIONS}}")

        else:
            title = SIGNATURE_TITLE_STANDARD

        if description:
            # Something was added to the description, so add the header
            description.insert(0, SIGNATURE_DESC_EXTENDED_HEADER)

        signature: str
        if not args_str and not modifiers:
            signature = f"{func_name}()"
        else:
            if not modifiers:
                signature = f"{func_name}( {args_str} )"
            else:
                indent = " " * (len(func_name) + 2)
                modifiers_str = "\n".join([f"{indent}{mod}" for mod in modifiers])
                signature = f"{func_name}( {args_str}\n{modifiers_str}\n{indent[:-2]})"

        return FunctionSignature(title=title, body=signature, description=description)

    def get_signatures(
        self,
        item: _registry_base.FunctionDocRegistryItem,
        env: GenerationEnvironment,
    ) -> FunctionSignatureCollection:
        func_name = item.name.upper()
        inf_args = False
        for defn in item.get_implementation_specs(env=env):
            inf_args = inf_args or (defn.arg_cnt is None)

        is_extended_syntax_values = [False]
        if supports_extensions(func_name, is_window=item.is_window):
            is_extended_syntax_values.append(True)

        args = item.get_args(env=env)
        signatures: List[FunctionSignature] = [
            self._get_signature_from_args(
                func_name=func_name,
                args=args,
                inf_args=inf_args,
                is_window=item.is_window,
                is_extended_syntax=is_extended_syntax,
                category_name=item.category_name,
            )
            for is_extended_syntax in is_extended_syntax_values
        ]
        sig_collection = FunctionSignatureCollection(
            placement_mode=self._placement_mode,
            signatures=signatures,
        )
        return sig_collection
