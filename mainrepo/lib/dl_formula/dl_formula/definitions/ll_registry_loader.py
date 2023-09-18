# TODO: Remove the `ll_` prefix

from itertools import chain
from typing import (
    Type,
    Union,
)

import attr

from dl_formula.definitions.base import NodeTranslation
from dl_formula.definitions.conditional_blocks import DEFINITIONS_COND_BLOCKS
from dl_formula.definitions.functions_aggregation import DEFINITIONS_AGGREGATION
from dl_formula.definitions.functions_array import DEFINITIONS_ARRAY
from dl_formula.definitions.functions_datetime import DEFINITIONS_DATETIME
from dl_formula.definitions.functions_logical import DEFINITIONS_LOGICAL
from dl_formula.definitions.functions_markup import DEFINITIONS_MARKUP
from dl_formula.definitions.functions_math import DEFINITIONS_MATH
from dl_formula.definitions.functions_special import DEFINITIONS_SPECIAL
from dl_formula.definitions.functions_string import DEFINITIONS_STRING
from dl_formula.definitions.functions_time_series import DEFINITIONS_TIME_SERIES
from dl_formula.definitions.functions_type import DEFINITIONS_TYPE
from dl_formula.definitions.functions_window import DEFINITIONS_WINDOW
from dl_formula.definitions.operators_binary import DEFINITIONS_BINARY
from dl_formula.definitions.operators_ternary import DEFINITIONS_TERNARY
from dl_formula.definitions.operators_unary import DEFINITIONS_UNARY
from dl_formula.definitions.registry import OPERATION_REGISTRY
from dl_formula.inspect.registry.item import BasicOpItem
from dl_formula.inspect.registry.registry import (
    LOWLEVEL_OP_REGISTRY,
    norm_name,
)


@attr.s
class _Flag:
    is_set: bool = attr.ib(default=False)


REGISTERED_FLAG = _Flag()


def populate_translation_registry() -> None:
    if REGISTERED_FLAG.is_set:
        # Already loaded
        return

    definitions = chain(
        DEFINITIONS_COND_BLOCKS,
        DEFINITIONS_AGGREGATION,
        DEFINITIONS_ARRAY,
        DEFINITIONS_DATETIME,
        DEFINITIONS_LOGICAL,
        DEFINITIONS_MARKUP,
        DEFINITIONS_MATH,
        DEFINITIONS_SPECIAL,
        DEFINITIONS_STRING,
        DEFINITIONS_TIME_SERIES,
        DEFINITIONS_TYPE,
        DEFINITIONS_WINDOW,
        DEFINITIONS_BINARY,
        DEFINITIONS_TERNARY,
        DEFINITIONS_UNARY,
    )
    def_item: Union[NodeTranslation, Type[NodeTranslation]]
    # TODO: Load defs from connectors here
    for def_item in definitions:
        if isinstance(def_item, type):
            def_item = def_item()
        assert isinstance(def_item, NodeTranslation)
        OPERATION_REGISTRY.register(def_item)

    REGISTERED_FLAG.is_set = True


def populate_ll_op_registry() -> None:
    for _key, impl_cls in OPERATION_REGISTRY.items():
        name = norm_name(impl_cls.name)
        assert name is not None
        ll_item = BasicOpItem(
            name=name,
            is_window=impl_cls.is_window,
            arg_cnt=impl_cls.arg_cnt,
            is_aggregate=impl_cls.is_aggregation,
            is_function=impl_cls.is_function,
            supports_grouping=impl_cls.supports_grouping,
            supports_ordering=impl_cls.supports_ordering,
            supports_ignore_dimensions=impl_cls.supports_ignore_dimensions,
            supports_bfb=impl_cls.supports_bfb,
            supports_lod=impl_cls.supports_lod,
            uses_default_ordering=impl_cls.uses_default_ordering,
        )
        LOWLEVEL_OP_REGISTRY.add(ll_item)
