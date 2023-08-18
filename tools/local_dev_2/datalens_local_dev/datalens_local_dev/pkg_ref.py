from pathlib import Path

import tomlkit


class ProjectRef:
    def __init__(self, file_path: Path):
        self.file_path = file_path
        self.data = None

    def load(self) -> None:
        with self.file_path.open("r") as f:
            self.data = tomlkit.parse(f.read())

    def get_local_dependencies(self, group: str = "default") -> dict[str, str]:
        dependencies = self.data["tool"]["poetry"]["dependencies"]
        local_dependencies = {}

        for name, version in dependencies.items():
            if isinstance(version, dict):
                if group in version:
                    if "path" in version[group]:
                        local_dependencies[name] = version[group]["path"]
                elif "path" in version:
                    local_dependencies[name] = version["path"]

        return local_dependencies

    def get_all_dependencies(self) -> dict[str, str]:
        dependency_groups = self.data["tool"]["poetry"].get("dependencies", {})
        all_dependencies = {}

        for group in dependency_groups:
            local_dependencies = self.get_local_dependencies(group)
            all_dependencies.update(local_dependencies)

        return all_dependencies

    def get_all_dependencies_rel_path(self) -> list[str]:
        result = []
        for name, path in self.get_all_dependencies().items():
            parts = Path(path).parts
            if len(parts) != 4:
                raise RuntimeError("Unexpected dependency structure, halt")

            result.append(f"{parts[-2]}/{parts[-1]}")

        result.append("lib/bi_connector_bigquery")  # workaround due to dependency hell in ops/ci/ deps
        # remove this once it's dealt with

        return result
