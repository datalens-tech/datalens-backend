import os
from typing import Optional

import attr
from frozendict import frozendict

from dl_repmanager.primitives import (
    PackageInfo, RequirementList, ReqPackageSpec, PypiReqPackageSpec, LocalReqPackageSpec,
)
from dl_repmanager.env import RepoEnvironment
from dl_repmanager.package_meta_reader import PackageMetaReader


@attr.s
class PackageIndex:
    _built: bool = attr.ib(kw_only=True, default=False)
    _package_infos_by_reg_name: dict[str, PackageInfo] = attr.ib(kw_only=True, factory=dict)
    _package_infos_by_module_name: dict[str, PackageInfo] = attr.ib(kw_only=True, factory=dict)
    _package_infos_by_test_name: dict[str, PackageInfo] = attr.ib(kw_only=True, factory=dict)
    _package_infos_by_path: dict[str, PackageInfo] = attr.ib(kw_only=True, factory=dict)

    def _is_built(self) -> bool:
        return self._built

    def get_package_info_by_reg_name(self, package_reg_name: str) -> PackageInfo:
        assert self._is_built()
        return self._package_infos_by_reg_name[package_reg_name]

    def get_package_info_from_path(self, rel_package_dir_name: str) -> PackageInfo:
        assert self._is_built()
        return self._package_infos_by_path[rel_package_dir_name]

    def get_package_info_from_module_name(self, package_module_name: str) -> PackageInfo:
        assert self._is_built()
        return self._package_infos_by_module_name[package_module_name]

    def get_package_info_from_test_name(self, test_dir_name: str) -> PackageInfo:
        assert self._is_built()
        return self._package_infos_by_test_name[test_dir_name]

    def list_package_infos(self) -> list[PackageInfo]:
        return list(self._package_infos_by_path.values())


@attr.s
class PackageIndexBuilder:
    repo_env: RepoEnvironment = attr.ib(kw_only=True)
    load_requirements: bool = attr.ib(kw_only=True, default=True)

    def _req_spec_from_dict(self, item_as_dict: dict) -> ReqPackageSpec:
        if 'path' in item_as_dict:
            return LocalReqPackageSpec(package_name=item_as_dict['name'], path=item_as_dict['path'])
        return PypiReqPackageSpec(package_name=item_as_dict['name'], version=item_as_dict['version'])

    def _load_package_info_from_package_dir(
            self, abs_package_dir_path: str, default_package_type: str
    ) -> Optional[PackageInfo]:

        toml_path = os.path.join(abs_package_dir_path, 'pyproject.toml')

        if not os.path.exists(toml_path):
            return None

        req_list_names = [
            'tool.poetry.dependencies',
            'tool.poetry.group.tests.dependencies',
        ]
        requirement_lists: dict[str, RequirementList] = {}
        with PackageMetaReader.from_file(toml_path) as pkg_meta_reader:
            package_reg_name = pkg_meta_reader.get_package_reg_name()
            module_names = pkg_meta_reader.get_package_module_names()
            package_type = pkg_meta_reader.get_package_type() or default_package_type
            if self.load_requirements:
                for req_list_name in req_list_names:
                    requirement_lists[req_list_name] = RequirementList(
                        req_specs=tuple(
                            self._req_spec_from_dict(item)
                            for item in pkg_meta_reader.iter_requirement_items(req_list_name)
                        ),
                    )

        test_dirs: list[str] = []
        for test_dir in os.listdir(abs_package_dir_path):  # TODO: read info about tests from pyproject.toml
            abs_test_dir = os.path.join(abs_package_dir_path, test_dir)
            if not os.path.isdir(abs_test_dir):
                continue
            if not 'tests' in test_dir or test_dir in module_names:
                continue  # it is not a test dir
            test_dirs.append(test_dir)

        package_info = PackageInfo(
            package_type=package_type,
            package_reg_name=package_reg_name,
            abs_path=abs_package_dir_path,
            module_names=module_names,
            test_dirs=tuple(test_dirs),
            requirement_lists=frozendict(requirement_lists),
        )
        return package_info

    def build_index(self) -> PackageIndex:
        package_infos_by_reg_name: dict[str, PackageInfo] = {}
        package_infos_by_module_name: dict[str, PackageInfo] = {}
        package_infos_by_test_name: dict[str, PackageInfo] = {}
        package_infos_by_path: dict[str, PackageInfo] = {}

        for package_type, abs_parent_dir_name in self.repo_env.iter_package_abs_dirs():
            for package_dir in os.listdir(abs_parent_dir_name):
                abs_package_dir_path = os.path.join(abs_parent_dir_name, package_dir)
                if not os.path.isdir(abs_package_dir_path):
                    continue
                package_info = self._load_package_info_from_package_dir(
                    abs_package_dir_path, default_package_type=package_type,
                )
                if package_info is None:
                    continue
                package_infos_by_reg_name[package_info.package_reg_name] = package_info
                package_infos_by_path[abs_package_dir_path] = package_info

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
