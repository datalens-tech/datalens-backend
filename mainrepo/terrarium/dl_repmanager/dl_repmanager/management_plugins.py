from __future__ import annotations

import abc
import os
import re
from typing import TYPE_CHECKING

import attr
import tomlkit

from dl_repmanager.primitives import PackageInfo
from dl_repmanager.fs_editor import FilesystemEditor
from dl_repmanager.toml_tools import TOMLWriter

if TYPE_CHECKING:
    from dl_repmanager.env import RepoEnvironment


@attr.s
class RepositoryManagementPlugin(abc.ABC):
    repo_env: RepoEnvironment = attr.ib(kw_only=True)
    base_path: str = attr.ib(kw_only=True)
    fs_editor: FilesystemEditor = attr.ib(init=False)

    @fs_editor.default
    def _make_fs_editor(self) -> FilesystemEditor:
        return self.repo_env.get_fs_editor()

    @abc.abstractmethod
    def register_package(self, package_info: PackageInfo) -> None:
        raise NotImplementedError


@attr.s
class CommonToolingRepositoryManagementPlugin(RepositoryManagementPlugin):
    _PACKAGE_LIST_REL_PATH = 'tools/local_dev/requirements/all_local_packages.lst'

    def register_package(self, package_info: PackageInfo) -> None:
        def transform_package_list(old_text: str) -> str:
            pkg_list = old_text.strip().split()
            pkg_rel_path = package_info.get_relative_path(self.base_path)
            pkg_list.append(pkg_rel_path)
            pkg_list.sort()
            return '\n'.join(pkg_list) + '\n'

        pkg_list_path = os.path.join(self.base_path, self._PACKAGE_LIST_REL_PATH)
        self.fs_editor.replace_file_content(pkg_list_path, replace_callback=transform_package_list)


@attr.s
class MainTomlRepositoryManagementPlugin(RepositoryManagementPlugin):
    _CI_TOML_REL_PATH = 'ops/ci/pyproject.toml'

    def _get_path_for_toml(self, package_info: PackageInfo) -> str:
        toml_abs_dir = os.path.dirname(os.path.join(self.base_path, self._CI_TOML_REL_PATH))
        pkg_rel_path = package_info.get_relative_path(toml_abs_dir)
        return pkg_rel_path

    def _register_main(self, toml_writer: TOMLWriter, package_info: PackageInfo):
        package_path_for_toml = self._get_path_for_toml(package_info)
        package_dep_table = tomlkit.inline_table()
        package_dep_table.add('path', package_path_for_toml)
        toml_writer.get_editable_section('tool.poetry.group.ci.dependencies').add(
            package_info.package_reg_name, package_dep_table)

    def _register_app(self, toml_writer: TOMLWriter, package_info: PackageInfo):
        package_path_for_toml = self._get_path_for_toml(package_info)
        package_base_name = os.path.basename(package_info.abs_path)
        package_dep_table = tomlkit.inline_table()
        package_dep_table.add('path', package_path_for_toml)
        section = toml_writer.add_section(f'tool.poetry.group.app_{package_base_name}.dependencies')
        section.add(package_info.package_reg_name, package_dep_table)
        section.add(tomlkit.nl())

    def register_package(self, package_info: PackageInfo) -> None:
        toml_path = os.path.join(self.base_path, self._CI_TOML_REL_PATH)

        with TOMLWriter.from_file(toml_path) as toml_writer:
            if 'main_dependency_group' in self.repo_env.get_tags(package_info.package_type):
                self._register_main(toml_writer=toml_writer, package_info=package_info)
            if 'own_dependency_group' in self.repo_env.get_tags(package_info.package_type):
                self._register_app(toml_writer=toml_writer, package_info=package_info)
