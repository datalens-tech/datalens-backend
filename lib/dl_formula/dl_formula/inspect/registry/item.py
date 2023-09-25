from typing import (
    NamedTuple,
    Optional,
)


class BasicOpItemKey(NamedTuple):
    name: str
    arg_cnt: Optional[int]
    is_function: bool
    is_aggregate: bool
    is_window: bool


class BasicOpItem(NamedTuple):
    """
    Low-level translator-independent function/operator registry
    for inspection and parsing purposes.
    """

    name: str
    arg_cnt: Optional[int]
    is_function: bool
    is_aggregate: bool
    is_window: bool
    uses_default_ordering: bool
    supports_grouping: bool
    supports_ordering: bool
    supports_lod: bool
    supports_ignore_dimensions: bool
    supports_bfb: bool


class NameIsWinOpItemKey(NamedTuple):
    name: str
    is_window: bool


class NameIsWinOpItem(NamedTuple):
    name: str
    is_window: bool
    uses_default_ordering: bool
    supports_grouping: bool
    supports_ordering: bool
    supports_lod: bool
    supports_ignore_dimensions: bool
    supports_bfb: bool


class NameOpItem(NamedTuple):
    name: str
    can_be_aggregate: bool
    can_be_window: bool
    can_be_nonwindow: bool
