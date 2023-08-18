"""
Load environment from config file (dl-repo.yml).
The file should have a structure similar to this:

    env:
      fs_editor: arc

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


Description of the sections:
  - include: section tells the loader to include another repo config file
    (e.g. for nested repositories).
  - package_types: defines packages types.
    This defines where to search for existing packages and how new packages are created.

"""


import os
from typing import ClassVar, Iterable, Type

import attr
import yaml

from dl_repmanager.fs_editor import FilesystemEditor, DefaultFilesystemEditor, ArcFilesystemEditor
from dl_repmanager.management_plugins import (
    RepositoryManagementPlugin,
    MainTomlRepositoryManagementPlugin,
    YaMakeRepositoryManagementPlugin,
    CommonToolingRepositoryManagementPlugin,
)


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

    package_types: dict[str, PackageTypeConfig] = attr.ib(kw_only=True)
    fs_editor: FilesystemEditor = attr.ib(kw_only=True)

    plugin_classes: ClassVar[tuple[Type[RepositoryManagementPlugin], ...]] = (
        YaMakeRepositoryManagementPlugin,
        CommonToolingRepositoryManagementPlugin,
        MainTomlRepositoryManagementPlugin,
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

    def get_plugins_for_package_type(self, package_type: str) -> list[RepositoryManagementPlugin]:
        # TODO: parameterize and load plugins from config
        home_repo_path = self.package_types[package_type].home_repo_path
        fs_editor = self.get_fs_editor()
        return [
            plugin_cls(repo_env=self, base_path=home_repo_path)
            for plugin_cls in self.plugin_classes
        ]

    def get_fs_editor(self) -> FilesystemEditor:
        return self.fs_editor


@attr.s
class RepoEnvironmentLoader:
    config_file_name: str = attr.ib(kw_only=True, default='dl-repo.yml')

    fs_editor_classes: ClassVar[dict[str, Type[FilesystemEditor]]] = {
        'default': DefaultFilesystemEditor,
        'arc': ArcFilesystemEditor,
    }

    def _load_params_from_yaml_file(self, config_path: str) -> tuple[dict, str]:
        with open(config_path) as config_file:
            config_data = yaml.safe_load(config_file)

        base_path = os.path.dirname(config_path)
        package_types: dict[str, PackageTypeConfig] = {}
        env_settings = config_data['env']
        for package_type_data in env_settings.get('package_types', ()):
            package_type = package_type_data['type']
            pkg_type_config = PackageTypeConfig(
                home_repo_path=base_path,
                path=os.path.join(base_path, package_type_data['root_path']),
                boilerplate_path=os.path.join(base_path, package_type_data['boilerplate_path']),
                tags=frozenset(package_type_data.get('tags', ())),
            )
            package_types[package_type] = pkg_type_config

        fs_editor_type = env_settings.get('fs_editor', 'default')

        for include in env_settings.get('include', ()):
            inc_package_types, _ = self._load_params_from_yaml_file(os.path.join(base_path, include))
            package_types.update(inc_package_types)

        return package_types, fs_editor_type  # FS editor is loaded only from the main config

    def _load_from_yaml_file(self, config_path: str) -> RepoEnvironment:
        package_types, fs_editor_type = self._load_params_from_yaml_file(config_path)
        fs_editor = self.fs_editor_classes[fs_editor_type]()
        return RepoEnvironment(package_types=package_types, fs_editor=fs_editor)

    def load_env(self, base_path: str) -> RepoEnvironment:
        return self._load_from_yaml_file(
            config_path=os.path.join(base_path, self.config_file_name)
        )
