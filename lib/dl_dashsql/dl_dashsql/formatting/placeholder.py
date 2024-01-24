"""
Query formatting with usage of placeholders
"""

import abc
import re

import attr

from dl_constants.enums import UserDataType
from dl_dashsql.formatting.base import (
    QueryBoundParameterWrapper,
    QueryFormatter,
    QueryFormattingState,
    QueryIncomingParameter,
    UnknownParameterPolicy,
)


@attr.s(frozen=True)
class PlaceholderStyle(abc.ABC):
    """Describes how to generate parameter placeholders"""

    @abc.abstractmethod
    def make_placeholder(self, param_name: str, param_idx: int, user_type: UserDataType) -> str:
        """Generate placeholder from the given parameter name and index (1-based)."""
        raise NotImplementedError


_VALID_PARAM_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]+$")
_VALID_PARAM_START_RE = re.compile(r"^[a-zA-Z]")
_INVALID_CHAR_RE = re.compile(r"[^a-zA-Z0-9_]")
_DEFAULT_PARAM_PREFIX = "p_"


@attr.s(frozen=True, kw_only=True)
class PlaceholderQueryFormatter(QueryFormatter):
    """
    Replaces parameters with customizable placeholders
    that somehow reference bound parameters.
    """

    _placeholder_style: PlaceholderStyle = attr.ib()

    def is_valid_param_name(self, param_name: str) -> bool:
        return bool(_VALID_PARAM_RE.match(param_name))

    def _generate_param_name(self, param_name: str, format_state: QueryFormattingState) -> str:
        new_param_name = param_name

        # Remove invalid characters
        new_param_name = _INVALID_CHAR_RE.sub("_", new_param_name)

        # Make sure it starts with a valid character
        # (the name can contain numbers, but can't start with them)
        if not _VALID_PARAM_START_RE.match(new_param_name):
            new_param_name = _DEFAULT_PARAM_PREFIX + new_param_name

        if new_param_name not in format_state.used_param_names:
            return new_param_name

        # Add a counter
        i = 0
        while True:
            new_param_name_counter = f"{new_param_name}_{i}"
            if new_param_name_counter not in format_state.used_param_names:
                new_param_name = new_param_name_counter
                break
            i += 1

        return new_param_name

    def make_param_replacement(
        self,
        original_text: str,
        param_name: str,
        format_state: QueryFormattingState,
    ) -> str:
        orig_param_name = param_name
        format_state.found_param_names.add(orig_param_name)

        incoming_param: QueryIncomingParameter
        if orig_param_name in format_state.incoming_param_dict:
            incoming_param = format_state.incoming_param_dict[orig_param_name]
        else:
            if self._unknown_param_policy == UnknownParameterPolicy.error:
                raise RuntimeError(f"Unknown parameter {orig_param_name}")
            if self._unknown_param_policy == UnknownParameterPolicy.ignore:
                # Do not perform any replacement
                return original_text
            else:
                raise ValueError(self._unknown_param_policy)

        new_param_name: str
        if orig_param_name in format_state.param_name_map:
            new_param_name = format_state.param_name_map[orig_param_name]
        elif self.is_valid_param_name(orig_param_name):
            new_param_name = orig_param_name
        else:
            new_param_name = self._generate_param_name(orig_param_name, format_state=format_state)
            format_state.param_name_map[orig_param_name] = new_param_name

        param_idx = len(format_state.bound_params) + 1
        param = QueryBoundParameterWrapper(
            incoming_param=incoming_param,
            param_name=new_param_name,
            param_idx=param_idx,
        )
        format_state.bound_params.append(param)
        format_state.used_param_names.add(new_param_name)

        placeholder = self._placeholder_style.make_placeholder(
            param_name=new_param_name,
            param_idx=param_idx,
            user_type=incoming_param.user_type,
        )
        return placeholder
