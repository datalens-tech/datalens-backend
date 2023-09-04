from __future__ import annotations

import abc
import os
from pathlib import Path
from typing import Any, Optional, TypeVar

import attr
from frozendict import frozendict
from tomlkit import inline_table


_REQ_SPEC_TV = TypeVar('_REQ_SPEC_TV', bound='ReqPackageSpec')


@attr.s(frozen=True)
class ReqPackageSpec(abc.ABC):
    package_name: str = attr.ib(kw_only=True)

    @abc.abstractmethod
    def pretty(self) -> str:
        raise NotImplementedError

    @abc.abstractmethod
    def as_req_str(self) -> str:
        raise NotImplementedError

    @abc.abstractmethod
    def to_toml_value(self) -> Any:
        raise NotImplementedError

    def clone(self: _REQ_SPEC_TV, **kwargs: Any) -> _REQ_SPEC_TV:
        return attr.evolve(self, **kwargs)


@attr.s(frozen=True)
class PypiReqPackageSpec(ReqPackageSpec):
    version: Optional[str] = attr.ib(kw_only=True)

    def pretty(self) -> str:
        extra = ''
        if self.version:
            extra = f' ({self.version})'
        return f'{self.package_name}{extra}'

    def is_exact_version(self) -> bool:
        return bool(self.version and self.version.startswith("=="))

    def get_exact_version(self) -> str:
        if not self.is_exact_version():
            raise ValueError(f"{str(self)} does not have an exact version")
        assert self.version is not None
        return self.version[2:]

    def as_req_str(self) -> str:
        if not self.version:
            return self.package_name
        return f'{self.package_name} = "{self.version}"'

    def to_toml_value(self) -> Any:
        return self.version if self.version else "*"


@attr.s(frozen=True)
class LocalReqPackageSpec(ReqPackageSpec):
    path: Path = attr.ib(kw_only=True)

    def pretty(self) -> str:
        return f'{self.package_name} ({self.path})'

    def as_req_str(self) -> str:
        return f'{self.package_name} = {{path = "{self.path}"}}'

    def to_toml_value(self) -> Any:
        it = inline_table()
        it["path"] = str(self.path)
        return it


@attr.s(frozen=True)
class RequirementList:
    req_specs: tuple[ReqPackageSpec, ...] = attr.ib(kw_only=True, default=())


@attr.s(frozen=True)
class PackageInfo:
    package_type: str = attr.ib(kw_only=True)
    package_reg_name: str = attr.ib(kw_only=True)
    abs_path: Path = attr.ib(kw_only=True)
    module_names: tuple[str, ...] = attr.ib(kw_only=True)
    test_dirs: tuple[str, ...] = attr.ib(kw_only=True, default=())
    requirement_lists: frozendict[str, RequirementList] = attr.ib(kw_only=True, default=frozendict())

    @property
    def toml_path(self) -> Path:
        return self.abs_path / 'pyproject.toml'

    @property
    def single_module_name(self) -> str:
        assert len(self.module_names) == 1
        return self.module_names[0]

    @property
    def single_test_dir(self) -> str:
        assert len(self.module_names) == 1
        return self.module_names[0]

    def get_relative_path(self, base_path: Path) -> Path:
        return Path(os.path.relpath(self.abs_path, base_path))

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
