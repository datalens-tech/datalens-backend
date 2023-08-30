from __future__ import annotations

from typing import List, Set

import attr

from bi_core.components.ids import FieldId


@attr.s
class FieldInterDependencyItem:
    dep_field_id: FieldId = attr.ib(kw_only=True)
    ref_field_ids: Set[FieldId] = attr.ib(kw_only=True, factory=set, converter=set)


@attr.s
class FieldInterDependencyInfo:
    """Stores direct (shallow) field-interdependencies."""

    deps: List[FieldInterDependencyItem] = attr.ib(factory=list)
