from __future__ import annotations

import abc
import os
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Type,
)

import attr
import tomlkit

from dl_repmanager.fs_editor import FilesystemEditor
from dl_repmanager.package_meta_reader import PackageMetaIOFactory
from dl_repmanager.primitives import (
    LocalReqPackageSpec,
    PackageInfo,
    RequirementList,
)
from dl_repmanager.toml_tools import (
    TOMLIOFactory,
    TOMLWriter,
)


if TYPE_CHECKING:
    from dl_repmanager.package_index import PackageIndex
    from dl_repmanager.repository_env import RepoEnvironment


@attr.s
class RepositoryManagementPlugin(abc.ABC):
    repository_env: RepoEnvironment = attr.ib(kw_only=True)
    package_index: PackageIndex = attr.ib(kw_only=True)
    base_path: Path = attr.ib(kw_only=True)
    config_data: dict = attr.ib(kw_only=True)
    fs_editor: FilesystemEditor = attr.ib(init=False)

    @fs_editor.default
    def _make_fs_editor(self) -> FilesystemEditor:
        return self.repository_env.get_fs_editor()

    @abc.abstractmethod
    def register_package(self, package_info: PackageInfo) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def unregister_package(self, package_info: PackageInfo) -> None:
        raise NotImplementedError

    def re_register_package(self, old_package_info: PackageInfo, new_package_info: PackageInfo) -> None:
        self.unregister_package(old_package_info)
        self.register_package(new_package_info)


@attr.s
class CommonToolingRepositoryManagementPlugin(RepositoryManagementPlugin):
    _package_list_rel_path: Path = attr.ib(init=False)

    @_package_list_rel_path.default
    def _make_package_list_rel_path(self) -> Path:
        return Path(self.config_data["package_list_path"])

    def register_package(self, package_info: PackageInfo) -> None:
        def transform_package_list(old_text: str) -> str:
            pkg_list = old_text.strip().split()
            pkg_rel_path = package_info.get_relative_path(self.base_path)
            pkg_list.append(str(pkg_rel_path))
            pkg_list.sort()
            return "\n".join(pkg_list) + "\n"

        pkg_list_path = self.base_path / self._package_list_rel_path
        self.fs_editor.replace_file_content(pkg_list_path, replace_callback=transform_package_list)

    def unregister_package(self, package_info: PackageInfo) -> None:
        def transform_package_list(old_text: str) -> str:
            pkg_list = old_text.strip().split()
            pkg_rel_path = package_info.get_relative_path(self.base_path)
            pkg_list.remove(str(pkg_rel_path))
            pkg_list.sort()
            return "\n".join(pkg_list) + "\n"

        pkg_list_path = self.base_path / self._package_list_rel_path
        self.fs_editor.replace_file_content(pkg_list_path, replace_callback=transform_package_list)


@attr.s
class MainTomlRepositoryManagementPlugin(RepositoryManagementPlugin):
    _metapackage_name: str = attr.ib(init=False)

    @_metapackage_name.default
    def _make_metapackage_name(self) -> str:
        return self.config_data["metapackage"]

    @property
    def _metapackage_path(self) -> Path:
        return self.repository_env.get_metapackage_spec(self._metapackage_name).toml_path

    def _get_path_for_toml(self, package_info: PackageInfo) -> Path:
        toml_abs_dir = (self.base_path / self._metapackage_path).parent
        return package_info.get_relative_path(toml_abs_dir)

    def _register_main(self, toml_writer: TOMLWriter, package_info: PackageInfo) -> None:
        package_path_for_toml = self._get_path_for_toml(package_info)
        package_dep_table = tomlkit.inline_table()
        package_dep_table.add("path", str(package_path_for_toml))
        toml_writer.get_editable_section("tool.poetry.group.ci.dependencies").add(
            package_info.package_reg_name, package_dep_table
        )

    def _unregister_main(self, toml_writer: TOMLWriter, package_info: PackageInfo):
        with toml_writer.suppress_non_existent_key():
            toml_writer.get_editable_section("tool.poetry.dependencies").remove(package_info.package_reg_name)
        with toml_writer.suppress_non_existent_key():
            toml_writer.get_editable_section("tool.poetry.group.dev.dependencies").remove(package_info.package_reg_name)
        with toml_writer.suppress_non_existent_key():
            toml_writer.get_editable_section("tool.poetry.group.ci.dependencies").remove(package_info.package_reg_name)

    def _register_app(self, toml_writer: TOMLWriter, package_info: PackageInfo) -> None:
        package_path_for_toml = self._get_path_for_toml(package_info)
        package_base_name = package_info.abs_path.name
        package_dep_table = tomlkit.inline_table()
        package_dep_table.add("path", str(package_path_for_toml))
        section = toml_writer.add_section(f"tool.poetry.group.app_{package_base_name}.dependencies")
        section.add(package_info.package_reg_name, package_dep_table)
        section.add(tomlkit.nl())

    def _unregister_app(self, toml_writer: TOMLWriter, package_info: PackageInfo) -> None:
        package_base_name = package_info.abs_path.name
        toml_writer.delete_section(f"tool.poetry.group.app_{package_base_name}.dependencies")

    def register_package(self, package_info: PackageInfo) -> None:
        toml_path = self.base_path / self._metapackage_path

        toml_io_factory = TOMLIOFactory(fs_editor=self.fs_editor)
        with toml_io_factory.toml_writer(toml_path) as toml_writer:
            if "main_dependency_group" in self.repository_env.get_tags(package_info.package_type):
                self._register_main(toml_writer=toml_writer, package_info=package_info)
            if "own_dependency_group" in self.repository_env.get_tags(package_info.package_type):
                self._register_app(toml_writer=toml_writer, package_info=package_info)

    def unregister_package(self, package_info: PackageInfo) -> None:
        toml_path = self.base_path / self._metapackage_path

        toml_io_factory = TOMLIOFactory(fs_editor=self.fs_editor)
        with toml_io_factory.toml_writer(toml_path) as toml_writer:
            if "main_dependency_group" in self.repository_env.get_tags(package_info.package_type):
                self._unregister_main(toml_writer=toml_writer, package_info=package_info)
            if "own_dependency_group" in self.repository_env.get_tags(package_info.package_type):
                self._unregister_app(toml_writer=toml_writer, package_info=package_info)


@attr.s
class DependencyReregistrationRepositoryManagementPlugin(RepositoryManagementPlugin):
    """Updates requirements in other packages dependent on this one"""

    def _is_package_dependent_on(
        self,
        section_name: str,
        dependent_package_info: PackageInfo,
        base_package_info: PackageInfo,
    ) -> bool:
        req_specs = dependent_package_info.requirement_lists.get(section_name, RequirementList()).req_specs
        for req_spec in req_specs:
            if req_spec.package_name == base_package_info.package_reg_name:
                # It really is a dependent package
                assert isinstance(req_spec, LocalReqPackageSpec)
                return True

        return False

    def register_package(self, package_info: PackageInfo) -> None:
        pass

    def unregister_package(self, package_info: PackageInfo) -> None:
        pass  # FIXME: Remove package from dependencies

    def re_register_package(self, old_package_info: PackageInfo, new_package_info: PackageInfo) -> None:
        package_meta_io_factory = PackageMetaIOFactory(fs_editor=self.fs_editor)

        # Scan other packages to see if they are dependent on this one and update these dependency entries
        for other_package_info in self.package_index.list_package_infos():
            if other_package_info == old_package_info:
                continue

            for section_name in other_package_info.requirement_lists:
                if other_package_info.is_dependent_on(old_package_info, section_name=section_name):
                    new_req_rel_path = Path(os.path.relpath(new_package_info.abs_path, other_package_info.abs_path))
                    with package_meta_io_factory.package_meta_writer(other_package_info.toml_path) as pkg_meta_writer:
                        pkg_meta_writer.update_requirement_item(
                            section_name=section_name,
                            item_name=old_package_info.package_reg_name,
                            new_item_name=new_package_info.package_reg_name,
                            new_path=new_req_rel_path,
                        )

        # Update own dependency entries if the package has moved to another dir
        old_pkg_dir_path = old_package_info.abs_path.parent
        new_pkg_dir_path = new_package_info.abs_path.parent
        if new_pkg_dir_path != old_pkg_dir_path:
            for section_name, req_list in old_package_info.requirement_lists.items():
                updated_requirements: dict[str, PackageInfo] = {}  # {<pkg_reg_name>: <req_package_info>}
                for other_package_spec in req_list.req_specs:
                    if not isinstance(other_package_spec, LocalReqPackageSpec):
                        continue
                    req_package_info = self.package_index.get_package_info_by_reg_name(other_package_spec.package_name)
                    updated_requirements[other_package_spec.package_name] = req_package_info

                with package_meta_io_factory.package_meta_writer(new_package_info.toml_path) as pkg_meta_writer:
                    for _req_package_reg_name, req_package_info in updated_requirements.items():
                        updated_req_path = Path(os.path.relpath(req_package_info.abs_path, new_package_info.abs_path))
                        pkg_meta_writer.update_requirement_item(
                            section_name=section_name,
                            item_name=req_package_info.package_reg_name,
                            new_item_name=req_package_info.package_reg_name,
                            new_path=updated_req_path,
                        )


_PLUGIN_CLASSES: dict[str, Type[RepositoryManagementPlugin]] = {
    "common_tooling": CommonToolingRepositoryManagementPlugin,
    "toml_registration": MainTomlRepositoryManagementPlugin,
    "dependency_registration": DependencyReregistrationRepositoryManagementPlugin,
}


def get_plugin_cls(plugin_type: str) -> Type[RepositoryManagementPlugin]:
    return _PLUGIN_CLASSES[plugin_type]
