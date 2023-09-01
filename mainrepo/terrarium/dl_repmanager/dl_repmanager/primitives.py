from __future__ import annotations

import abc
import os
from typing import Any, Optional

import attr
from frozendict import frozendict
from tomlkit import inline_table


@attr.s(frozen=True)
class ReqPackageSpec(abc.ABC):
    package_name: str = attr.ib(kw_only=True)

    @abc.abstractmethod
    def pretty(self) -> str:
        raise NotImplementedError

    @abc.abstractmethod
    def to_toml_value(self) -> Any:
        raise NotImplementedError


@attr.s(frozen=True)
class PypiReqPackageSpec(ReqPackageSpec):
    version: Optional[str] = attr.ib(kw_only=True)

    def pretty(self) -> str:
        extra = ''
        if self.version:
            extra = f' ({self.version})'
        return f'{self.package_name}{extra}'

    def is_exact_version(self) -> bool:
        return bool(self.version) and self.version.startswith("==")

    def get_exact_version(self) -> str:
        if not self.is_exact_version():
            raise ValueError(f"{str(self)} does not have an exact version")
        return self.version[2:]

    def to_toml_value(self) -> Any:
        return self.version if self.version else "*"


@attr.s(frozen=True)
class LocalReqPackageSpec(ReqPackageSpec):
    path: str = attr.ib(kw_only=True)

    def pretty(self) -> str:
        return f'{self.package_name} ({self.path})'

    def to_toml_value(self) -> Any:
        it = inline_table()
        it["path"] = self.path
        return it


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
    def toml_path(self) -> str:
        return os.path.join(self.abs_path, 'pyproject.toml')

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

    def is_dependent_on(
            self,
            base_package_info: PackageInfo,
            section_name: str,
    ) -> bool:
        req_specs = self.requirement_lists.get(section_name, RequirementList()).req_specs
        for req_spec in req_specs:
            if req_spec.package_name == base_package_info.package_reg_name:
                # It really is a dependent package
                assert isinstance(req_spec, LocalReqPackageSpec)
                return True

        return False
