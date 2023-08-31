"""
Load environment from config file (dl-repo.yml).
The file should have a structure similar to this:

    dl_repo:
      fs_editor: git

      include:
        - core_repo/dl-repo.yml

      package_types:
        - type: lib
          root_path: lib
          boilerplate_path: lib/bi_package_boilerplate

        - type: app
          root_path: app
          boilerplate_path: lib/bi_package_boilerplate
          tags:
            - own_dependency_group

      custom_package_map:
        flask_marshmallow: flask-marshmallow
        jwt: pyjwt


Description of the sections:
  - include: section tells the loader to include another repo config file
    (e.g. for nested repositories).
  - fs_editor: allows customization of FS operations
    so that move/copy actions are immediately registered in the VCS
  - package_types: defines packages types.
    This defines where to search for existing packages and how new packages are created.
  - custom_package_map: custom mapping of third-party module to package names
    (for validation of package requirements)

"""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar, Iterable, Optional, Type

import attr
import yaml

from dl_repmanager.fs_editor import DefaultFilesystemEditor, FilesystemEditor, GitFilesystemEditor
from dl_repmanager.management_plugins import (
    CommonToolingRepositoryManagementPlugin,
    DependencyReregistrationRepositoryManagementPlugin,
    MainTomlRepositoryManagementPlugin,
    RepositoryManagementPlugin,
)

if TYPE_CHECKING:
    from dl_repmanager.package_index import PackageIndex


DEFAULT_CONFIG_FILE_NAME = 'dl-repo.yml'


@attr.s(frozen=True)
class PackageTypeConfig:
    home_repo_path: str = attr.ib(kw_only=True)
    path: str = attr.ib(kw_only=True)
    boilerplate_path: str = attr.ib(kw_only=True)
    tags: frozenset[str] = attr.ib(kw_only=True, default=frozenset())


@attr.s(frozen=True)
class RepoEnvironment:
    """
    Provides information about repository folders and boilerplates
    """

    base_path: str = attr.ib(kw_only=True)
    package_types: dict[str, PackageTypeConfig] = attr.ib(kw_only=True)
    custom_package_map: dict[str, str] = attr.ib(kw_only=True, factory=dict)
    fs_editor: FilesystemEditor = attr.ib(kw_only=True)

    plugin_classes: ClassVar[tuple[Type[RepositoryManagementPlugin], ...]] = (
        CommonToolingRepositoryManagementPlugin,
        MainTomlRepositoryManagementPlugin,
        DependencyReregistrationRepositoryManagementPlugin,
    )

    def iter_package_abs_dirs(self) -> Iterable[tuple[str, str]]:
        return sorted(
            [
                (package_type, pkg_type_config.path)
                for package_type, pkg_type_config in self.package_types.items()
            ],
            key=lambda pair: pair[0]
        )

    def get_boilerplate_package_dir(self, package_type: str) -> str:
        return self.package_types[package_type].boilerplate_path

    def get_root_package_dir(self, package_type: str) -> str:
        return self.package_types[package_type].path

    def get_tags(self, package_type: str) -> frozenset[str]:
        return self.package_types[package_type].tags

    def get_plugins_for_package_type(
            self, package_type: str, package_index: PackageIndex,
    ) -> list[RepositoryManagementPlugin]:
        # TODO: parameterize and load plugins from config
        home_repo_path = self.package_types[package_type].home_repo_path
        return [
            plugin_cls(
                repo_env=self,
                package_index=package_index,
                pkg_type_base_path=home_repo_path,
                base_path=self.base_path,
            )
            for plugin_cls in self.plugin_classes
        ]

    def get_fs_editor(self) -> FilesystemEditor:
        return self.fs_editor


_DEFAULT_FS_EDITOR_TYPE = 'default'


@attr.s(frozen=True)
class ConfigContents:
    base_path: str = attr.ib(kw_only=True)
    package_types: dict[str, PackageTypeConfig] = attr.ib(kw_only=True, factory=dict)
    custom_package_map: dict[str, str] = attr.ib(kw_only=True, factory=dict)
    fs_editor_type: str = attr.ib(kw_only=True, default=_DEFAULT_FS_EDITOR_TYPE)


def discover_config(base_path: Path, config_file_name: str) -> Path:
    while base_path and base_path.exists():
        config_path = base_path / config_file_name
        if config_path.exists():
            return config_path
        base_path = base_path.parent

    raise RuntimeError('Failed to discover the repo config file. This does not seem to be a managed repository.')


@attr.s
class RepoEnvironmentLoader:
    config_file_name: str = attr.ib(kw_only=True, default=DEFAULT_CONFIG_FILE_NAME)

    fs_editor_classes: ClassVar[dict[str, Type[FilesystemEditor]]] = {
        'default': DefaultFilesystemEditor,
        'git': GitFilesystemEditor,
    }

    def _load_params_from_yaml_file(self, config_path: Path) -> ConfigContents:
        with open(config_path) as config_file:
            config_data = yaml.safe_load(config_file)

        base_path = os.path.dirname(config_path)
        package_types: dict[str, PackageTypeConfig] = {}
        env_settings = config_data.get('dl_repo', {})
        for package_type_data in env_settings.get('package_types', ()):
            package_type = package_type_data['type']
            pkg_type_config = PackageTypeConfig(
                home_repo_path=base_path,
                path=os.path.join(base_path, package_type_data['root_path']),
                boilerplate_path=os.path.join(base_path, package_type_data['boilerplate_path']),
                tags=frozenset(package_type_data.get('tags', ())),
            )
            package_types[package_type] = pkg_type_config

        custom_package_map: dict[str, str] = dict(env_settings.get('custom_package_map', {}))

        fs_editor_type: Optional[str] = env_settings.get('fs_editor')

        for include in env_settings.get('include', ()):
            included_config_contents = self._load_params_from_yaml_file(base_path / include)
            package_types = dict(included_config_contents.package_types, **package_types)
            custom_package_map = dict(included_config_contents.custom_package_map, **custom_package_map)
            fs_editor_type = fs_editor_type or included_config_contents.fs_editor_type

        # FS editor is loaded only from the main config
        return ConfigContents(
            base_path=base_path,
            package_types=package_types,
            custom_package_map=custom_package_map,
            fs_editor_type=fs_editor_type,
        )

    def _load_from_yaml_file(self, config_path: Path) -> RepoEnvironment:
        config_contents = self._load_params_from_yaml_file(config_path)
        fs_editor_type = config_contents.fs_editor_type or _DEFAULT_FS_EDITOR_TYPE
        assert fs_editor_type is not None
        fs_editor = self.fs_editor_classes[fs_editor_type]()
        return RepoEnvironment(
            base_path=config_contents.base_path,
            package_types=config_contents.package_types,
            custom_package_map=config_contents.custom_package_map,
            fs_editor=fs_editor,
        )

    def load_env(self, base_path: str) -> RepoEnvironment:
        return self._load_from_yaml_file(
            config_path=discover_config(Path(base_path), self.config_file_name)
        )
