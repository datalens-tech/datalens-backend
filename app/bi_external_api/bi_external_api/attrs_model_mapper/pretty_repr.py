import enum
import functools
from typing import (
    Any,
    Collection,
    Mapping,
    Optional,
    Sequence,
    Type,
    Union,
)

import attr

from bi_external_api.structs.mappings import FrozenMappingStrToStrOrStrSeq
from bi_external_api.structs.singleormultistring import SingleOrMultiString

_INDENT = " " * 4


def pretty_repr(
    model: Any,
    preferred_cls_name_prefixes: Union[Mapping[Any, Optional[str]], Sequence[Any], None] = None,
) -> str:
    """
    Generates string with prettily-formatted executable code that will create model passed in `model`.
    Classes that are declared as module-level vars in `preferred_cls_name_prefixes` will be prefixed with module name.
    Order matters. First appearance takes precedence. Take into account that imports are treated as module-level vars.
    """
    effective_preferred_cls_name_prefixes: Mapping[Any, Optional[str]]

    if isinstance(preferred_cls_name_prefixes, dict):
        effective_preferred_cls_name_prefixes = preferred_cls_name_prefixes
    elif preferred_cls_name_prefixes is None:
        effective_preferred_cls_name_prefixes = {}
    else:
        effective_preferred_cls_name_prefixes = {mod_obj: None for mod_obj in preferred_cls_name_prefixes}

    lines = Renderer(effective_preferred_cls_name_prefixes).get_lines(model)
    return "\n".join(lines)


@attr.s(frozen=True, auto_attribs=True)
class _DictItem:
    key: Any
    value: Any


@attr.s()
class Renderer:
    _preferred_cls_name_prefixes: Mapping[Any, Optional[str]] = attr.ib()
    _map_cls_cls_prefix: dict[Type, str] = attr.ib(init=False, factory=dict)

    def __attrs_post_init__(self) -> None:
        for module_obj in self._preferred_cls_name_prefixes.keys():
            module_name = module_obj.__name__.split(".")[-1]
            declared_type_list: list[Type] = [var for var in vars(module_obj).values() if isinstance(var, type)]
            for declared_type in declared_type_list:
                preferred_prefix = self._preferred_cls_name_prefixes.get(module_obj)
                effective_prefix = module_name if preferred_prefix is None else preferred_prefix

                self._map_cls_cls_prefix[declared_type] = effective_prefix

    def get_type_str(self, t: Type) -> str:
        if t in self._map_cls_cls_prefix:
            return f"{self._map_cls_cls_prefix[t]}.{t.__name__}"
        return t.__name__

    @functools.singledispatchmethod
    def _get_lines_internal(self, model: Any) -> list[str]:
        if attr.has(type(model)):
            return self._get_lines_for_attrs_object(model)

        raise NotImplementedError(f"No pretty-repr generator for {type(model)=}")

    @_get_lines_internal.register
    def _get_lines_internal_dict_item(self, model: _DictItem) -> list[str]:
        key_lines = self._get_lines_internal(model.key)
        value_lines = self._get_lines_internal(model.value)

        return [
            *key_lines[:-1],
            f"{key_lines[-1]}: {value_lines[0]}",
            *value_lines[1:],
        ]

    @_get_lines_internal.register(int)
    @_get_lines_internal.register(float)
    @_get_lines_internal.register(str)
    @_get_lines_internal.register(type(None))
    @_get_lines_internal.register(SingleOrMultiString)
    def _get_lines_internal_primitive(self, model: Union[int, float, str, None]) -> list[str]:
        return [repr(model)]

    @_get_lines_internal.register
    def _get_lines_internal_enum(self, model: enum.Enum) -> list[str]:
        return [f"{self.get_type_str(type(model))}.{model.name}"]

    @_get_lines_internal.register
    def _get_lines_internal_list(self, model: list) -> list[str]:
        return self._get_lines_for_simple_collection(
            model,
            start="[",
            end="]",
        )

    @_get_lines_internal.register
    def _get_lines_internal_tuple(self, model: tuple) -> list[str]:
        return self._get_lines_for_simple_collection(
            model,
            start="(",
            end=")",
            trailing_comma_required=True,
        )

    @_get_lines_internal.register
    def _get_lines_internal_set(self, model: set) -> list[str]:
        return self._get_lines_for_simple_collection(model, start="{", end="}", empty_override="set()")

    @_get_lines_internal.register
    def _get_lines_internal_dict(self, model: dict) -> list[str]:
        return self._get_lines_for_simple_collection(
            [_DictItem(key=key, value=value) for key, value in model.items()],
            start="{",
            end="}",
            inline_single_element=False,
        )

    @_get_lines_internal.register
    def _get_lines_internal_frozen_mapping_str_to_str_or_str_seq(
        self,
        model: FrozenMappingStrToStrOrStrSeq,
    ) -> list[str]:
        under_hood_dict_lines = self._get_lines_internal(dict(model))
        prefix = f"{self.get_type_str(type(model))}("
        suffix = ")"

        if len(under_hood_dict_lines) == 1:
            return [f"{prefix}{under_hood_dict_lines[0]}{suffix}"]

        return [
            f"{prefix}{under_hood_dict_lines[0]}",
            *under_hood_dict_lines[1:-1],
            f"{under_hood_dict_lines[-1]}{suffix}",
        ]

    def _get_lines_for_attrs_object(self, model: Any) -> list[str]:
        the_type = type(model)
        lines: list[str] = [f"{self.get_type_str(the_type)}("]
        for field_name, field in attr.fields_dict(the_type).items():
            nested_lines = self._get_lines_internal(getattr(model, field_name))
            assert len(nested_lines) > 0

            first_line = f"{_INDENT}{field_name}={nested_lines[0]}"

            if len(nested_lines) == 1:
                lines.append(first_line + ",")
            elif len(nested_lines) == 2:
                lines.append(first_line)
                lines.append(f"{_INDENT}{nested_lines[1]},")
            else:
                lines.append(first_line)
                lines.extend(f"{_INDENT}{the_nested_line}" for the_nested_line in nested_lines[1:-1])
                lines.append(f"{_INDENT}{nested_lines[-1]},")
        lines.append(")")
        return lines

    def _get_lines_for_simple_collection(
        self,
        model: Collection,
        start: str,
        end: str,
        trailing_comma_required: bool = False,
        empty_override: Optional[str] = None,
        inline_single_element: bool = True,
    ) -> list[str]:
        if len(model) == 0:
            if empty_override is not None:
                return [empty_override]

            return [f"{start}{end}"]

        first_item_lines = self._get_lines_internal(next(iter(model)))

        if len(model) == 1 and len(first_item_lines) == 1 and inline_single_element:
            return [f"{start}{first_item_lines[0]}{', ' if trailing_comma_required else ''}{end}"]

        all_lines = [start]
        for seq_item in model:
            seq_item_lines = self._get_lines_internal(seq_item)
            all_lines.extend([f"{_INDENT}{the_seq_item_line}" for the_seq_item_line in seq_item_lines[:-1]])
            all_lines.append(f"{_INDENT}{seq_item_lines[-1]},")

        all_lines.append(end)
        return all_lines

    def get_lines(self, model: Any) -> list[str]:
        return self._get_lines_internal(model)
