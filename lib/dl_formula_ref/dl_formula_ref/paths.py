from __future__ import annotations

import os.path
from typing import (
    NamedTuple,
    Optional,
)

import attr

from dl_formula_ref.reference import FuncReference
from dl_formula_ref.registry.registry import RefFunctionKey
from dl_formula_ref.texts import HUMAN_CATEGORIES


class FileLink(NamedTuple):
    name: str
    path: str


class FuncPathTemplate(NamedTuple):
    path: str

    def format(self, category_name: str, func_name: str):  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
        return self.path.format(func_name=func_name, category_name=category_name)


class CatPathTemplate(NamedTuple):
    path: str

    def format(self, category_name: str):  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
        return self.path.format(category_name=category_name)


@attr.s
class PathRenderer:
    _cat_path_template: CatPathTemplate = attr.ib(kw_only=True)
    _func_path_template: FuncPathTemplate = attr.ib(kw_only=True)
    _func_ref: FuncReference = attr.ib(kw_only=True)
    _base_path: str = attr.ib(kw_only=True, default="")

    def _append_path(self, name: str) -> str:
        if not self._base_path:
            return name
        return os.path.join(self._base_path, name)

    def _get_relative_path(self, path: str) -> str:
        if not self._base_path:
            return path
        base_parts = self._base_path.replace(os.sep, "/").split("/")
        child_parts = path.replace(os.sep, "/").split("/")
        common_part_cnt = 0
        while common_part_cnt < min(len(base_parts), len(child_parts)):
            if base_parts[-(common_part_cnt + 1) :] != child_parts[: common_part_cnt + 1]:
                break
            common_part_cnt += 1

        result = os.path.join(*child_parts[common_part_cnt:])
        return result

    def child(self, name: str) -> PathRenderer:
        return attr.evolve(self, base_path=self._append_path(name))

    def get_func_path(self, func_key: RefFunctionKey, anchor_name: Optional[str] = None) -> str:
        raw_func = self._func_ref.get_func(func_key=func_key)
        filename = raw_func.internal_name.upper()
        path = self._get_relative_path(
            self._func_path_template.format(
                func_name=filename,
                category_name=raw_func.category.name,
            )
        )
        if anchor_name:
            path = f"{path}#{anchor_name}"
        return path

    def get_cat_path(self, category_name: str, anchor_name: Optional[str] = None) -> str:
        path = self._get_relative_path(
            self._cat_path_template.format(
                category_name=category_name,
            )
        )
        if anchor_name:
            path = f"{path}#{anchor_name}"
        return path

    def get_func_link(
        self,
        func_name: str,
        category_name: Optional[str] = None,
        anchor_name: Optional[str] = None,
    ) -> FileLink:
        if category_name is None:
            found_in_categories = [raw_func.category.name for raw_func in self._func_ref.filter(name=func_name)]
            if len(found_in_categories) > 1:
                raise ValueError(
                    "Function with name {} found in following categories: {} - but category is not specified. "
                    'Use the following syntax to specify category: "{{ref:category_name/FUNC_NAME[:ref text]}}"'
                    "".format(func_name, found_in_categories)
                )
            category_name = found_in_categories[0]
        func_key = RefFunctionKey.normalized(name=func_name, category_name=category_name)
        return FileLink(
            name=func_name.upper(),
            path=self.get_func_path(func_key=func_key, anchor_name=anchor_name),
        )

    def get_cat_link(
        self,
        category_name: str,
        anchor_name: Optional[str] = None,
    ) -> FileLink:
        return FileLink(
            name=HUMAN_CATEGORIES.get(category_name, category_name),
            path=self.get_cat_path(category_name=category_name, anchor_name=anchor_name),
        )
