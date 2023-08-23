from __future__ import annotations

import abc
import os
from typing import Any, Optional

import attr
from frozendict import frozendict


@attr.s(frozen=True)
class ReqPackageSpec(abc.ABC):
    package_name: str = attr.ib(kw_only=True)

    @abc.abstractmethod
    def pretty(self) -> str:
        raise NotImplementedError


@attr.s(frozen=True)
class PypiReqPackageSpec(ReqPackageSpec):
    version: Optional[str] = attr.ib(kw_only=True)

    def pretty(self) -> str:
        extra = ''
        if self.version:
            extra = f' ({self.version})'
        return f'{self.package_name}{extra}'


@attr.s(frozen=True)
class LocalReqPackageSpec(ReqPackageSpec):
    path: str = attr.ib(kw_only=True)

    def pretty(self) -> str:
        return f'{self.package_name} ({self.path})'


@attr.s(frozen=True)
class RequirementList:
    req_specs: tuple[ReqPackageSpec, ...] = attr.ib(kw_only=True, default=())


@attr.s(frozen=True)
class PackageInfo:
    package_type: str = attr.ib(kw_only=True)
    package_reg_name: str = attr.ib(kw_only=True)
    abs_path: str = attr.ib(kw_only=True)
    module_names: tuple[str, ...] = attr.ib(kw_only=True)
    test_dirs: tuple[str, ...] = attr.ib(kw_only=True, default=())
    requirement_lists: frozendict[str, RequirementList] = attr.ib(kw_only=True, default=frozendict())

    @property
    def single_module_name(self) -> str:
        assert len(self.module_names) == 1
        return self.module_names[0]

    @property
    def single_test_dir(self) -> str:
        assert len(self.module_names) == 1
        return self.module_names[0]

    def get_relative_path(self, base_path: str) -> str:
        return os.path.relpath(self.abs_path, base_path)

    def clone(self, **kwargs: Any) -> PackageInfo:
        return attr.evolve(self, **kwargs)
