from __future__ import annotations

from collections import defaultdict
from typing import (
    Generator,
    NamedTuple,
    Optional,
    Sequence,
)

from dl_formula.core.datatype import DataType
from dl_formula.core.dialect import DialectCombo
from dl_formula.core.dialect import StandardDialect as D
import dl_formula.core.exc as exc
import dl_formula.definitions.base as op_base
from dl_formula.definitions.scope import Scope


class FuncKey(NamedTuple):
    name: str
    arg_cnt: Optional[int]
    is_window: bool

    @property
    def unlimited_arg_version(self) -> FuncKey:
        return self._replace(arg_cnt=None)


class OperationRegistry:
    def __init__(self):
        self.ops: dict[FuncKey, list["op_base.NodeTranslation"]] = {}

    def __contains__(self, key: FuncKey) -> bool:
        return key in self.ops

    def __getitem__(self, key: FuncKey) -> list:
        return self.ops[key]

    def __bool__(self) -> bool:
        return bool(self.ops)

    def _func_key_from_def_item(self, def_item: op_base.NodeTranslation) -> FuncKey:
        name = def_item.name
        assert name is not None
        return FuncKey(
            name=name,
            arg_cnt=def_item.arg_cnt,
            is_window=def_item.is_window,
        )

    def register(self, def_item: "op_base.NodeTranslation") -> None:
        assert isinstance(def_item, op_base.NodeTranslation)
        if def_item.name is None:
            raise ValueError("Cannot register nameless functions")
        func_key = self._func_key_from_def_item(def_item)
        if func_key not in self.ops:
            self.ops[func_key] = []
        if def_item not in self.ops[func_key]:
            self.ops[func_key].append(def_item)

    def unregister(self, def_item: "op_base.NodeTranslation") -> None:
        if def_item.name is None:
            raise ValueError("Cannot unregister nameless functions")
        func_key = self._func_key_from_def_item(def_item)
        self.ops[func_key].remove(def_item)
        if not self.ops[func_key]:
            del self.ops[func_key]

    def get_definition(
        self,
        *,
        name: str,
        arg_types: Sequence[DataType],
        is_window: bool = False,
        dialect: Optional[DialectCombo] = None,
        for_any_dialect: bool = False,
        required_scopes: int = Scope.EXPLICIT_USAGE,
    ) -> "op_base.NodeTranslation":
        """Find translation that matches given name and arguments"""

        if dialect is not None and for_any_dialect or dialect is None and not for_any_dialect:
            raise ValueError(
                "Either dialect should be provided or for_any_dialect be set to True." " Cannot provide both."
            )

        arg_types = list(arg_types)
        func_key = FuncKey(name=name.lower(), arg_cnt=len(arg_types), is_window=is_window)

        # to also find versions of the F with unlimited number of args:
        inf_func_key = func_key.unlimited_arg_version
        translations = [
            func_translation
            for func_translation in self.ops.get(func_key, []) + self.ops.get(inf_func_key, [])
            if func_translation.scopes & required_scopes == required_scopes
        ]
        if not translations:
            # function not found
            raise exc.TranslationUnknownFunctionError(
                f"Unknown {len(arg_types)}-argument function or operator {name.upper()}"
            )

        some_type_match = False

        for func_translation in translations:
            if func_translation.match_types(arg_types):
                if dialect is None:
                    assert for_any_dialect
                    return func_translation
                elif func_translation.match_dialect(dialect):
                    return func_translation
                else:
                    some_type_match = True

        # such a function does exist, but there is no implementation that accepts the given arguments
        what = "function" if translations[0].is_function else "operator"
        what = f"window {what}" if is_window else what
        arg_types_repr = ", ".join(arg_type.name for arg_type in arg_types)
        if some_type_match:
            message_comment = f" the {what} is only defined for other databases"
        else:
            message_comment = f" no such {what} currently exists"
        message_parts = [
            f"Invalid argument types for {what} {name.upper()}",
            f"dialect: {dialect.common_name_and_version if dialect is not None else None}",
            f"types: ({arg_types_repr})",
            message_comment,
        ]
        raise exc.DataTypeError("; ".join(message_parts) + ".")

    def items(self) -> Generator[tuple[FuncKey, "op_base.NodeTranslation"], None, None]:
        """Like regular ``dict.items()``,  but flatten translation lists"""
        sorted_keys = sorted(
            self.ops.keys(), key=lambda key: (key[0], key[1] if key[1] is not None else float("inf"), *key[2:])
        )
        for func_key in sorted_keys:
            trans_list = self.ops[func_key]
            for translation in trans_list:
                yield func_key, translation

    def get_supported_functions(
        self,
        require_dialects: DialectCombo = D.EMPTY,
        only_functions: bool = True,
        function_scopes: int = Scope.EXPLICIT_USAGE,
    ) -> list[FuncKey]:
        """
        Return a list of supported functions for given combination of dialects (``require_dialects``).
        Result is a list of (<func_name>, <arg_cnt>) tuples
        (keys from ``OPERATION_REGISTRY``).
        """
        support_by_key = defaultdict(lambda: D.EMPTY)  # type: ignore  # TODO: fix
        for func_key, func_tr in self.items():
            if func_tr.scopes & function_scopes != function_scopes:
                continue
            if only_functions and not func_tr.is_function:
                continue
            support_by_key[func_key] |= func_tr.supported_dialects()

        result = []
        sorted_keys = sorted(
            support_by_key.keys(), key=lambda key: (key[0], key[1] if key[1] is not None else float("inf"), *key[2:])
        )
        for func_key in sorted_keys:
            supp_dialects = support_by_key[func_key]
            if supp_dialects & require_dialects == require_dialects:
                result.append(func_key)

        return result


OPERATION_REGISTRY = OperationRegistry()
