import itertools
import os.path
from pathlib import Path
import subprocess
from typing import Optional
from urllib.parse import urlparse

import attr
import tomlkit
from tomlkit import TOMLDocument
from tomlkit.items import Key

from dl_repmanager.primitives import (
    LocalReqPackageSpec,
    PypiReqPackageSpec,
    ReqPackageSpec,
)
from dl_repmanager.toml_tools import (
    TOMLReader,
    TOMLWriter,
)


@attr.s()
class MetaPackageManager:
    dir_path: Path = attr.ib()
    _toml: TOMLDocument = attr.ib(default=None)

    def __attrs_post_init__(self) -> None:
        toml_path = self.dir_path / "pyproject.toml"

        if self._toml is None:
            with open(toml_path) as fp:
                self._toml = tomlkit.load(fp)

    def get_reader(self) -> TOMLReader:
        return TOMLReader(toml=self._toml)

    def get_writer(self) -> TOMLWriter:
        return TOMLWriter(toml=self._toml)

    def as_new_location(self, new_path: Path) -> "MetaPackageManager":
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
        return list(self._toml["tool"]["poetry"]["group"].keys())

    def remove_poetry_group(self, name: str) -> None:
        del self._toml["tool"]["poetry"]["group"][name]

    def list_mypy_stubs_override(self) -> list[str]:
        mypy_section = self._toml["datalens"]["meta"]["mypy_stubs_packages_override"]
        return list(mypy_section.keys())

    def remove_mypy_stubs_override(self, name: str) -> None:
        mypy_section = self._toml["datalens"]["meta"]["mypy_stubs_packages_override"]
        del mypy_section[name]

    def remove_package_sources_section(self) -> None:
        del self._toml["tool"]["poetry"]["source"]

    @staticmethod
    def get_dependencies_section_name(group: Optional[str]) -> str:
        if group is None:
            return "tool.poetry.dependencies"
        return f"tool.poetry.group.{group}.dependencies"

    def get_dependencies(self, group: Optional[str] = None) -> list[ReqPackageSpec]:
        r = self.get_reader()

        ret: list[ReqPackageSpec] = []

        for key_str, val in r.iter_section_items(self.get_dependencies_section_name(group)):
            if key_str == "python":
                continue

            if isinstance(val, str):
                ret.append(PypiReqPackageSpec(package_name=key_str, version=val))
            elif isinstance(val, dict):
                if "path" in val:
                    ret.append(LocalReqPackageSpec(package_name=key_str, path=Path(val["path"])))
                else:
                    raise ValueError(f"No path in local dependency: {key_str}: {val}")
            else:
                raise ValueError(f"Unknown types of key/val in dependency: {key_str}: {val}")
        return ret

    def export_dependencies_raw(self, group: str) -> str:
        proc = subprocess.run(
            [
                "poetry",
                "export",
                "--only",
                group,
                "--without-hashes",
                "--format",
                "requirements.txt",
            ],
            cwd=self.dir_path,
            stdout=subprocess.PIPE,
            check=True,
        )
        dependencies = proc.stdout.decode("ascii")
        return dependencies

    def export_dependencies(self, group: str) -> list[ReqPackageSpec]:
        raw_deps = self.export_dependencies_raw(group)
        ret: list[ReqPackageSpec] = []
        for line in raw_deps.splitlines():
            if " ; " not in line:
                continue
            dep = line.split(";")[0].strip()
            if "@" in dep:
                pkg_name, pkg_url = dep.split("@")
                ret.append(
                    LocalReqPackageSpec(
                        package_name=pkg_name.strip(),
                        path=Path(
                            os.path.relpath(
                                urlparse(pkg_url.strip()).path,
                                self.dir_path,
                            )
                        ),
                    )
                )
            else:
                pkg_name = dep.split("==")[0]
                ret.append(PypiReqPackageSpec(package_name=pkg_name, version=dep.removeprefix(pkg_name)))
        return ret

    def run_poetry_lock(self, suppress_stdout: bool = False) -> None:
        stdout_target = subprocess.DEVNULL if suppress_stdout else None
        subprocess.run(["poetry", "lock", "--no-update"], cwd=self.dir_path, check=True, stdout=stdout_target)

    def save(self) -> None:
        file_path = self.dir_path / "pyproject.toml"

        with open(file_path, "w") as fp:
            fp.write(tomlkit.dumps(self._toml))
