from pathlib import Path
from typing import Optional

import attr
from frozendict import frozendict

from dl_repmanager.package_meta_reader import PackageMetaIOFactory
from dl_repmanager.primitives import (
    LocalReqPackageSpec,
    PackageInfo,
    PypiReqPackageSpec,
    ReqPackageSpec,
    RequirementList,
)
from dl_repmanager.repository_env import RepoEnvironment


@attr.s
class PackageIndex:
    _built: bool = attr.ib(kw_only=True, default=False)
    _package_infos_by_reg_name: dict[str, PackageInfo] = attr.ib(kw_only=True, factory=dict)
    _package_infos_by_module_name: dict[str, PackageInfo] = attr.ib(kw_only=True, factory=dict)
    _package_infos_by_test_name: dict[str, PackageInfo] = attr.ib(kw_only=True, factory=dict)
    _package_infos_by_path: dict[Path, PackageInfo] = attr.ib(kw_only=True, factory=dict)

    def _is_built(self) -> bool:
        return self._built

    def get_package_info_by_reg_name(self, package_reg_name: str) -> PackageInfo:
        assert self._is_built()
        return self._package_infos_by_reg_name[package_reg_name]

    def get_package_info_from_path(self, rel_package_dir_name: Path) -> PackageInfo:
        assert self._is_built()
        return self._package_infos_by_path[rel_package_dir_name]

    def get_package_info_from_module_name(self, package_module_name: str) -> PackageInfo:
        assert self._is_built()
        return self._package_infos_by_module_name[package_module_name]

    def get_package_info_from_test_name(self, test_dir_name: str) -> PackageInfo:
        assert self._is_built()
        return self._package_infos_by_test_name[test_dir_name]

    def list_package_infos(self, package_type: Optional[str] = None) -> list[PackageInfo]:
        return [
            package_info
            for package_info in self._package_infos_by_path.values()
            if package_type is None or package_info.package_type == package_type
        ]


@attr.s
class PackageIndexBuilder:
    repository_env: RepoEnvironment = attr.ib(kw_only=True)
    load_requirements: bool = attr.ib(kw_only=True, default=True)

    def _req_spec_from_dict(self, item_as_dict: dict) -> ReqPackageSpec:
        if "path" in item_as_dict:
            return LocalReqPackageSpec(package_name=item_as_dict["name"], path=item_as_dict["path"])
        return PypiReqPackageSpec(package_name=item_as_dict["name"], version=item_as_dict["version"])

    def _load_package_info_from_package_dir(
        self, abs_package_dir_path: Path, default_package_type: str
    ) -> Optional[PackageInfo]:
        toml_path = abs_package_dir_path / "pyproject.toml"

        if not toml_path.exists():
            return None

        req_list_names = [
            "tool.poetry.dependencies",
            "tool.poetry.group.tests.dependencies",
        ]
        requirement_lists: dict[str, RequirementList] = {}
        package_meta_io_factory = PackageMetaIOFactory(fs_editor=self.repository_env.get_fs_editor())
        with package_meta_io_factory.package_meta_reader(toml_path) as pkg_meta_reader:
            package_reg_name = pkg_meta_reader.get_package_reg_name()
            module_names = pkg_meta_reader.get_package_module_names()
            package_type = pkg_meta_reader.get_package_type() or default_package_type
            implicit_reqs = frozenset(pkg_meta_reader.get_implicit_dependencies())
            if self.load_requirements:
                for req_list_name in req_list_names:
                    requirement_lists[req_list_name] = RequirementList(
                        req_specs=tuple(
                            self._req_spec_from_dict(item)
                            for item in pkg_meta_reader.iter_requirement_items(req_list_name)
                        ),
                    )

        test_dirs: list[str] = []
        for test_dir in abs_package_dir_path.iterdir():  # TODO: read info about tests from pyproject.toml
            if not test_dir.is_dir():
                continue
            if "tests" not in test_dir.name or test_dir.name in module_names:
                continue  # it is not a test dir
            test_dirs.append(test_dir.name)

        package_info = PackageInfo(
            package_type=package_type,
            package_reg_name=package_reg_name,
            abs_path=abs_package_dir_path,
            module_names=module_names,
            test_dirs=tuple(test_dirs),
            requirement_lists=frozendict(requirement_lists),
            implicit_deps=implicit_reqs,
        )
        return package_info

    def build_index(self) -> PackageIndex:
        package_infos_by_reg_name: dict[str, PackageInfo] = {}
        package_infos_by_module_name: dict[str, PackageInfo] = {}
        package_infos_by_test_name: dict[str, PackageInfo] = {}
        package_infos_by_path: dict[Path, PackageInfo] = {}

        for package_type, abs_parent_dir in self.repository_env.iter_package_abs_dirs():
            for package_dir in abs_parent_dir.iterdir():
                if not package_dir.is_dir():
                    continue

                package_info = self._load_package_info_from_package_dir(
                    package_dir,
                    default_package_type=package_type,
                )
                if package_info is None:
                    continue
                package_infos_by_reg_name[package_info.package_reg_name] = package_info
                package_infos_by_path[package_dir] = package_info

                for module_name in package_info.module_names:
                    package_infos_by_module_name[module_name] = package_info

                for test_dir in package_info.test_dirs:
                    package_infos_by_test_name[test_dir] = package_info

        package_index = PackageIndex(
            built=True,
            package_infos_by_reg_name=package_infos_by_reg_name,
            package_infos_by_path=package_infos_by_path,
            package_infos_by_module_name=package_infos_by_module_name,
            package_infos_by_test_name=package_infos_by_test_name,
        )
        return package_index
