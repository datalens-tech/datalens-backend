from pathlib import Path
from pprint import pprint

import attrs
import tomlkit
from packaging.requirements import Requirement


# import toml  # todo: replace with tomlkit


@attrs.define(slots=False)
class LocalRef:
    root_path: Path
    section: str
    dir_name: str
    pkg_name: str

    def partial_path(self) -> str:
        return f"{self.section}/{self.dir_name}"

    def get_pkg_ref(self) -> 'PkgRef':
        return PkgRef(self.root_path / self.section / self.dir_name)

    def get_direct_local_deps(self):
        ref = self.get_pkg_ref()


@attrs.define(slots=False)
class PkgRef:
    path: Path

    @property
    def root_path(self) -> Path:
        return self.path.parent.parent

    @property
    def self_toml(self):
        try:
            return tomlkit.load(open(self.path / "pyproject.toml"))
        except Exception as err:
            print(err)
            return None

    @property
    def raw_dependencies(self):
        # print(self.path)
        try:
            data = self.self_toml
            return data["tool"]["poetry"]["dependencies"]
        except Exception:
            return []

    def extract_requirements(self) -> list[Requirement] | None:
        spec = self.self_toml
        if spec:
            return [Requirement(x) for x in spec["tool"]["poetry"]["dependencies"]]
        return None

    def get_local_dependencies(self) -> list[LocalRef]:
        # not used yet ...
        spec = self.self_toml
        libs_reqs: list[Requirement] = []
        if spec:
            for group in spec["tool"]["poetry"]["group"]:
                pprint(group)
        #
        #
        #     for x in spec["tool"]["poetry"]["dev-dependencies"]["lib"]:
        #         local_dep = x.strip()
        #         if local_dep.startswith("-e "):
        #             local_dep = local_dep[3:]
        #         libs_reqs.append(Requirement(local_dep))
        #
        # result = []
        # for req in libs_reqs:
        #     if not req.url.startswith("file:///"):
        #         raise ValueError(f"Got non file dev-lib dependency: {req.__dict__()}")
        #
        #     parts = req.url.split("/")
        #     if len(parts) < 2:
        #         raise ValueError(f"Got bad url in dev-lib dependency: : {req.__dict__()}")
        #
        #     section, dir_name = parts[-2], parts[-1]
        #     result.append(LocalRef(
        #         root_path=self.root_path,
        #         section=section,
        #         dir_name=dir_name,
        #         pkg_name=req.name,
        #     ))

    @property
    def self_pkg_name(self):

        try:
            return self.self_toml["tool"]["poetry"]["name"]
        except (KeyError, TypeError):
            return None

    @property
    def ci_label(self) -> str:
        parts = self.path.parts
        if len(parts) <= 2:
            return str(Path)
        if parts[-2] in SECTIONS:
            return f"{parts[-2]}>{parts[-1]}"
        return parts[-1]

    @property
    def rel_path(self) -> str:
        parts = self.path.parts
        if len(parts) <= 2:
            return "/".join(parts)
        if parts[-2] in SECTIONS:
            return f"{parts[-2]}/{parts[-1]}"
        return parts[-1]

    @property
    def skip_test(self) -> bool:
        spec = self.self_toml
        try:
            return spec["datalens_ci"]["skip_test"]
        except (KeyError, TypeError):
            return False

        test_paths = []
        #  [tool.pytest.ini_options]
        try:
            test_paths = spec["tool"]["pytest"]["ini_options"]["testpaths"]
        except KeyError:
            pass

        return len(test_paths) > 0


SECTIONS = ("lib", "app", "ops")
PREFIX = "yandex-"
