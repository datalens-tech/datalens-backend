import itertools
import os.path
from pathlib import Path
from typing import Optional

import attr
import tomlkit
from tomlkit import TOMLDocument
from tomlkit.items import Key

from dl_repmanager.primitives import ReqPackageSpec, PypiReqPackageSpec, LocalReqPackageSpec
from dl_repmanager.toml_tools import TOMLReader, TOMLWriter


@attr.s()
class MetaPackageManager:
    dir_path: str = attr.ib()
    _toml: TOMLDocument = attr.ib(default=None)

    def __attrs_post_init__(self) -> None:
        toml_path = os.path.join(self.dir_path, "pyproject.toml")

        if self._toml is None:
            with open(toml_path) as fp:
                self._toml = tomlkit.load(fp)

    def get_reader(self) -> TOMLReader:
        return TOMLReader(toml=self._toml)

    def get_writer(self) -> TOMLWriter:
        return TOMLWriter(toml=self._toml)

    def as_new_location(self, new_path: str) -> "MetaPackageManager":
        clone = MetaPackageManager(
            dir_path=new_path,
            toml=tomlkit.loads(tomlkit.dumps(self._toml)),
        )

        for group in itertools.chain([None], self.list_poetry_groups()):
            for dependency in self.get_dependencies(group):
                if isinstance(dependency, LocalReqPackageSpec):
                    dep_abs_path = Path(self.dir_path) / dependency.path
                    dep_new_rel_path = Path(os.path.relpath(dep_abs_path, new_path))

                    clone.write_dependency(group, attr.evolve(dependency, path=dep_new_rel_path))

        return clone

    def remove_dependency(self, group_name: Optional[str], pkg: str | ReqPackageSpec) -> None:
        w = self.get_writer()
        section = w.get_editable_section(self.get_dependencies_section_name(group_name))

        effective_pkg_name = pkg if isinstance(pkg, str) else pkg.package_name
        section.remove(effective_pkg_name)

    def write_dependency(self, group_name: Optional[str], pkg_spec: ReqPackageSpec) -> None:
        w = self.get_writer()
        section = w.get_editable_section(self.get_dependencies_section_name(group_name))
        section[pkg_spec.package_name] = pkg_spec.to_toml_value()

    def list_poetry_groups(self) -> list[str]:
        return list(
            self._toml["tool"]["poetry"]["group"].keys()
        )

    def remove_poetry_group(self, name: str) -> None:
        del self._toml["tool"]["poetry"]["group"][name]

    @staticmethod
    def get_dependencies_section_name(group: Optional[str]) -> str:
        if group is None:
            return "tool.poetry.dependencies"
        return f"tool.poetry.group.{group}.dependencies"

    def get_dependencies(self, group: Optional[str] = None) -> list[ReqPackageSpec]:
        r = self.get_reader()

        ret: list[ReqPackageSpec] = []

        for key, val in r.iter_section_items(self.get_dependencies_section_name(group)):
            if not isinstance(key, Key):
                continue

            key_str = key.key
            if key_str == "python":
                continue

            if isinstance(val, str):
                ret.append(PypiReqPackageSpec(package_name=key.key, version=val))
            elif isinstance(val, dict):
                if "path" in val:
                    ret.append(LocalReqPackageSpec(package_name=key.key, path=Path(val["path"])))
                else:
                    raise ValueError(f"No path in local dependency: {key}: {val}")
            else:
                raise ValueError(f"Unknown types of key/val in dependency: {key}: {val}")
        return ret

    def save(self) -> None:
        file_path = os.path.join(self.dir_path, "pyproject.toml")

        with open(file_path, "w") as fp:
            fp.write(tomlkit.dumps(self._toml))
