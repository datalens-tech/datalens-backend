"""
Load environment from config file (dl-repo.yml).
The file should have a structure similar to this:

    dl_repo:
      fs_editor: git

      include:
        - core_repo/dl-repo.yml

      default_boilerplate_path: lib/dl_package_boilerplate

      package_types:
        - type: lib
          root_path: lib

        - type: app
          root_path: app
          boilerplate_path: app/app_package_boilerplate
          tags:
            - own_dependency_group

      custom_package_map:
        flask_marshmallow: flask-marshmallow
        jwt: pyjwt

      metapackages:
        - name: main
          toml: tools/pyproject.toml

      plugins:
        - type: dependency_registration

      edit_exclude_masks:
        - ".*\\.mo",


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

from pathlib import Path
import re
from typing import (
    TYPE_CHECKING,
    Iterable,
    Optional,
)

import attr
import yaml

from dl_repmanager.fs_editor import (
    FilesystemEditor,
    get_fs_editor,
)
from dl_repmanager.management_plugins import (
    RepositoryManagementPlugin,
    get_plugin_cls,
)
from dl_repmanager.primitives import MetaPackageSpec


if TYPE_CHECKING:
    from dl_repmanager.package_index import PackageIndex


DEFAULT_CONFIG_FILE_NAME = "dl-repo.yml"


@attr.s(frozen=True)
class PackageTypeConfig:
    home_repo_path: Path = attr.ib(kw_only=True)
    path: Path = attr.ib(kw_only=True)
    boilerplate_path: Path = attr.ib(kw_only=True)
    tags: frozenset[str] = attr.ib(kw_only=True, default=frozenset())


@attr.s(frozen=True)
class PluginConfig:
    plugin_type: str = attr.ib(kw_only=True)
    config_data: dict = attr.ib(kw_only=True)


@attr.s(frozen=True)
class RepoEnvironment:
    """
    Provides information about repository folders and boilerplates
    """

    base_path: Path = attr.ib(kw_only=True)
    package_types: dict[str, PackageTypeConfig] = attr.ib(kw_only=True)
    metapackages: dict[str, MetaPackageSpec] = attr.ib(kw_only=True)
    custom_package_map: dict[str, str] = attr.ib(kw_only=True, factory=dict)
    fs_editor: FilesystemEditor = attr.ib(kw_only=True)
    plugin_configs: list[PluginConfig] = attr.ib(kw_only=True, factory=list)
    edit_exclude_masks: frozenset[re.Pattern] = attr.ib(kw_only=True, default=frozenset())

    def iter_package_abs_dirs(self) -> Iterable[tuple[str, Path]]:
        return sorted(
            [(package_type, pkg_type_config.path) for package_type, pkg_type_config in self.package_types.items()],
            key=lambda pair: pair[0],
        )

    def get_boilerplate_package_dir(self, package_type: str) -> Path:
        return self.package_types[package_type].boilerplate_path

    def get_root_package_dir(self, package_type: str) -> Path:
        return self.package_types[package_type].path

    def get_tags(self, package_type: str) -> frozenset[str]:
        return self.package_types[package_type].tags

    def get_plugins(
        self,
        package_index: PackageIndex,
    ) -> list[RepositoryManagementPlugin]:
        # TODO: parameterize and load plugins from config
        return [
            get_plugin_cls(plugin_type=plugin_config.plugin_type)(
                repository_env=self,
                package_index=package_index,
                base_path=self.base_path,
                config_data=plugin_config.config_data,
            )
            for plugin_config in self.plugin_configs
        ]

    def get_fs_editor(self) -> FilesystemEditor:
        return self.fs_editor

    def get_metapackage_spec(self, metapackage_name: str) -> MetaPackageSpec:
        return self.metapackages[metapackage_name]

    def get_edit_exclude_masks(self) -> frozenset[re.Pattern]:
        return self.edit_exclude_masks


_DEFAULT_FS_EDITOR_TYPE = "default"


@attr.s(frozen=True)
class ConfigContents:
    base_path: Path = attr.ib(kw_only=True)
    package_types: dict[str, PackageTypeConfig] = attr.ib(kw_only=True, factory=dict)
    metapackages: dict[str, MetaPackageSpec] = attr.ib(kw_only=True, factory=dict)
    custom_package_map: dict[str, str] = attr.ib(kw_only=True, factory=dict)
    fs_editor_type: Optional[str] = attr.ib(kw_only=True, default=_DEFAULT_FS_EDITOR_TYPE)
    plugin_configs: list[PluginConfig] = attr.ib(kw_only=True, factory=list)
    edit_exclude_masks: frozenset[re.Pattern] = attr.ib(kw_only=True, default=frozenset())


def discover_config(base_path: Path, config_file_name: str) -> Path:
    while base_path and base_path.exists():
        config_path = base_path / config_file_name
        if config_path.exists():
            return config_path
        base_path = base_path.parent

    raise RuntimeError("Failed to discover the repo config file. This does not seem to be a managed repository.")


@attr.s
class RepoEnvironmentLoader:
    config_file_name: str = attr.ib(kw_only=True, default=DEFAULT_CONFIG_FILE_NAME)
    override_fs_editor_type: Optional[str] = attr.ib(kw_only=True, default=None)

    def _load_params_from_yaml_file(self, config_path: Path) -> ConfigContents:
        with open(config_path) as config_file:
            config_data = yaml.safe_load(config_file)

        base_path = config_path.parent
        env_settings = config_data.get("dl_repo", {})

        default_boilerplate_path_str: Optional[str] = env_settings.get("default_boilerplate_path")

        package_types: dict[str, PackageTypeConfig] = {}
        for package_type_data in env_settings.get("package_types", ()):
            package_type = package_type_data["type"]
            boilerplate_path_str = package_type_data.get("boilerplate_path", default_boilerplate_path_str)
            if boilerplate_path_str is None:
                raise ValueError("Boilerplate must be specified in package type or default")
            pkg_type_config = PackageTypeConfig(
                home_repo_path=base_path,
                path=base_path / package_type_data["root_path"],
                boilerplate_path=base_path / boilerplate_path_str,
                tags=frozenset(package_type_data.get("tags", ())),
            )
            package_types[package_type] = pkg_type_config

        metapackages: dict[str, MetaPackageSpec] = {}
        for metapackage_data in env_settings.get("metapackages", ()):
            metapackage_name = metapackage_data["name"]
            metapackage_config = MetaPackageSpec(
                name=metapackage_name,
                toml_path=base_path / metapackage_data["toml"],
            )
            metapackages[metapackage_name] = metapackage_config

        plugin_configs: list[PluginConfig] = []
        for plugin_data in env_settings.get("plugins", ()):
            plugin_type = plugin_data["type"]
            plugin_config = PluginConfig(
                plugin_type=plugin_type,
                config_data=plugin_data.get("config") or {},
            )
            plugin_configs.append(plugin_config)

        edit_exclude_masks: set[re.Pattern] = set()
        for edit_exclude_masks_item_str in env_settings.get("edit_exclude_masks", ()):
            edit_exclude_masks.add(re.compile(edit_exclude_masks_item_str))

        custom_package_map: dict[str, str] = dict(env_settings.get("custom_package_map", {}))

        fs_editor_type: Optional[str] = env_settings.get("fs_editor")

        for include in env_settings.get("include", ()):
            included_config_contents = self._load_params_from_yaml_file(Path(base_path) / include)
            package_types = dict(included_config_contents.package_types, **package_types)
            metapackages = dict(included_config_contents.metapackages, **metapackages)
            custom_package_map = dict(included_config_contents.custom_package_map, **custom_package_map)
            edit_exclude_masks |= included_config_contents.edit_exclude_masks
            fs_editor_type = fs_editor_type or included_config_contents.fs_editor_type

        # FS editor and plugins are loaded only from the top-level config
        return ConfigContents(
            base_path=base_path,
            package_types=package_types,
            metapackages=metapackages,
            custom_package_map=custom_package_map,
            fs_editor_type=fs_editor_type,
            plugin_configs=plugin_configs,
            edit_exclude_masks=frozenset(edit_exclude_masks),
        )

    def _load_from_yaml_file(self, config_path: Path) -> RepoEnvironment:
        config_contents = self._load_params_from_yaml_file(config_path)
        base_path = config_contents.base_path

        fs_editor_type = config_contents.fs_editor_type or _DEFAULT_FS_EDITOR_TYPE
        if self.override_fs_editor_type is not None:
            fs_editor_type = self.override_fs_editor_type
        assert fs_editor_type is not None

        return RepoEnvironment(
            base_path=base_path,
            package_types=config_contents.package_types,
            metapackages=config_contents.metapackages,
            custom_package_map=config_contents.custom_package_map,
            plugin_configs=config_contents.plugin_configs,
            edit_exclude_masks=config_contents.edit_exclude_masks,
            fs_editor=get_fs_editor(
                fs_editor_type=fs_editor_type,
                base_path=base_path,
            ),
        )

    def load_env(self, base_path: Path) -> RepoEnvironment:
        return self._load_from_yaml_file(config_path=discover_config(base_path, self.config_file_name))
