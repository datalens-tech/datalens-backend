# TODO: Move to dl_core.components.*

from __future__ import annotations

from typing import (
    Dict,
    List,
    Optional,
    Sequence,
    Type,
)

import attr

from dl_constants.enums import (
    ComponentErrorLevel,
    ComponentType,
)


@attr.s
class ComponentError:
    level: ComponentErrorLevel = attr.ib()
    message: str = attr.ib()
    code: List[str] = attr.ib(factory=list)
    details: dict = attr.ib(factory=dict)


@attr.s
class ComponentErrorPack:
    id: str = attr.ib()
    type: ComponentType = attr.ib()
    errors: List[ComponentError] = attr.ib(factory=list)

    def get_errors(
        self,
        code: Optional[Sequence[str]] = None,
        code_prefix: Optional[Sequence[str]] = None,
    ) -> List[ComponentError]:
        if code is not None and code_prefix is not None:
            raise ValueError("Cannot specify both code and code_prefix")

        if code is not None:
            code = list(code)
            cond = lambda err: err.code == code
        elif code_prefix is not None:
            code_prefix = list(code_prefix)
            cond = lambda err: err.code[: len(code_prefix)] == code_prefix  # type: ignore  # TODO: fix
        else:
            cond = lambda err: True
        return [err for err in self.errors if cond(err)]

    def remove_errors(
        self,
        code: Optional[Sequence[str]] = None,
        code_prefix: Optional[Sequence[str]] = None,
    ) -> None:
        for err in self.get_errors(code=code, code_prefix=code_prefix):
            self.errors.remove(err)


# FIXME: switch to subclass for fields (with row, column, token)
ERROR_CLS_BY_COMP_TYPE: Dict[ComponentType, Type[ComponentError]] = {
    ComponentType.data_source: ComponentError,
    ComponentType.source_avatar: ComponentError,
    ComponentType.avatar_relation: ComponentError,
    ComponentType.field: ComponentError,
    ComponentType.obligatory_filter: ComponentError,
    ComponentType.result_schema: ComponentError,
}


@attr.s
class ComponentErrorRegistry:
    items: List[ComponentErrorPack] = attr.ib(factory=list)

    def get_pack(self, id: str) -> Optional[ComponentErrorPack]:
        for item in self.items:
            if item.id == id:
                return item

    def remove_errors(
        self,
        id: str,
        code: Optional[Sequence[str]] = None,
        code_prefix: Optional[Sequence[str]] = None,
    ) -> None:
        """
        Remove errors for object with given ID.
        if code is given, remove only errors having the given error code.
        """
        item = self.get_pack(id=id)
        if item is None:
            return

        item.remove_errors(code=code, code_prefix=code_prefix)
        if not item.errors:
            self.items.remove(item)

    def add_error(  # type: ignore  # TODO: fix
        self,
        id: str,
        type: ComponentType,
        message: str,
        code: Sequence[str],
        level: ComponentErrorLevel = ComponentErrorLevel.error,
        details: Optional[dict] = None,
    ):
        details = details or {}
        error_cls = ERROR_CLS_BY_COMP_TYPE.get(type)
        if error_cls is None:
            raise ValueError(f"Invalid value for type: {type}")

        item = self.get_pack(id=id)
        if item is None:
            item = ComponentErrorPack(id=id, type=type)
            self.items.append(item)

        item.errors.append(error_cls(message=message, code=list(code), level=level, details=details))

    def for_type(self, type: ComponentType) -> List[ComponentErrorPack]:
        return [item for item in self.items if item.type == type]

    def rename_pack(self, old_id: str, new_id: str) -> None:
        for item in self.items:
            if item.id == old_id:
                item.id = new_id
