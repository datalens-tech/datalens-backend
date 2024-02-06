from __future__ import annotations

import abc
from ast import AST
from enum import (
    Enum,
    auto,
)
import os
from pathlib import Path
from typing import (
    Any,
    Optional,
    TypeVar,
)

import attr
from frozendict import frozendict
from tomlkit import inline_table


_CLONABLE_TV = TypeVar("_CLONABLE_TV", bound="_Clonable")


@attr.s(frozen=True)
class _Clonable(abc.ABC):  # noqa: B024
    def clone(self: _CLONABLE_TV, **kwargs: Any) -> _CLONABLE_TV:
        return attr.evolve(self, **kwargs)


class EntityReferenceType(Enum):
    package_type = auto()
    package_reg = auto()
    module = auto()
    path = auto()


@attr.s(frozen=True)
class EntityReference:
    ref_type: EntityReferenceType = attr.ib(kw_only=True)
    name: str = attr.ib(kw_only=True)


@attr.s(frozen=True)
class ReqPackageSpec(_Clonable):
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


@attr.s(frozen=True)
class PypiReqPackageSpec(ReqPackageSpec):
    version: Optional[str] = attr.ib(kw_only=True)

    def pretty(self) -> str:
        extra = ""
        if self.version:
            extra = f" ({self.version})"
        return f"{self.package_name}{extra}"

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
        return f"{self.package_name} ({self.path})"

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
class LocaleDomainSpec(_Clonable):
    domain_name: str = attr.ib(kw_only=True)
    scan_paths: tuple[Path, ...] = attr.ib(kw_only=True)


@attr.s(frozen=True)
class PackageInfo(_Clonable):
    package_type: str = attr.ib(kw_only=True)
    package_reg_name: str = attr.ib(kw_only=True)
    abs_path: Path = attr.ib(kw_only=True)
    module_names: tuple[str, ...] = attr.ib(kw_only=True)
    test_dirs: tuple[str, ...] = attr.ib(kw_only=True, default=())
    requirement_lists: frozendict[str, RequirementList] = attr.ib(kw_only=True, default=frozendict())
    implicit_deps: frozenset[str] = attr.ib(kw_only=True, default=frozenset())
    i18n_domains: tuple[LocaleDomainSpec, ...] = attr.ib(kw_only=True, default=())

    @property
    def toml_path(self) -> Path:
        return self.abs_path / "pyproject.toml"

    @property
    def single_module_name(self) -> str:
        assert len(self.module_names) == 1
        return self.module_names[0]

    @property
    def single_test_dir(self) -> str:
        assert len(self.test_dirs) == 1
        return self.test_dirs[0]

    @property
    def reg_entity_ref(self) -> EntityReference:
        return EntityReference(
            ref_type=EntityReferenceType.package_reg,
            name=self.package_reg_name,
        )

    @property
    def package_type_entity_ref(self) -> EntityReference:
        return EntityReference(
            ref_type=EntityReferenceType.package_type,
            name=self.package_type,
        )

    @property
    def tests_and_modules(self) -> tuple[str, ...]:
        return tuple(*self.module_names, *self.test_dirs)

    def get_relative_path(self, base_path: Path) -> Path:
        return Path(os.path.relpath(self.abs_path, base_path))

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


@attr.s(frozen=True)
class ImportSpec:
    import_ast: AST = attr.ib(kw_only=True)
    import_module_name: str = attr.ib(kw_only=True)
    source_path: Path = attr.ib(kw_only=True)


@attr.s(frozen=True)
class MetaPackageSpec(_Clonable):
    name: str = attr.ib(kw_only=True)
    toml_path: Path = attr.ib(kw_only=True)
