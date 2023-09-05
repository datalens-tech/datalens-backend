from functools import cached_property
from pathlib import Path

import attrs
import tomlkit


@attrs.define(slots=False)
class PkgRef:
    root: Path
    full_path: Path

    @cached_property
    def partial_parent_path(self):
        return self.full_path.relative_to(self.root)

    @cached_property
    def self_toml(self):
        try:
            return tomlkit.load(open(self.full_path / "pyproject.toml"))
        except Exception as err:
            print(err)
            return None

    def extract_local_requirements(self, include_groups: list[str] | None = None) -> set[str]:
        """
        Returns pkg names listed in dependencies of current pkg
        """
        spec = self.self_toml
        result = set()
        raw = {}
        if spec:
            raw = dict(spec["tool"]["poetry"]["dependencies"])
            for group in include_groups or []:
                if "group" not in spec["tool"]["poetry"]:
                    continue
                raw.update(spec["tool"]["poetry"]["group"].get(group, {}).get("dependencies", {}))

        for name, specifier in raw.items():
            if isinstance(specifier, dict) and "path" in specifier:
                result.add(name)

        return result

    @cached_property
    def self_pkg_name(self):
        try:
            return self.self_toml["tool"]["poetry"]["name"]
        except (KeyError, TypeError):
            return None

    @property
    def skip_test(self) -> bool:
        spec = self.self_toml
        try:
            return spec["datalens_ci"]["skip_test"]
        except (KeyError, TypeError):
            return False
