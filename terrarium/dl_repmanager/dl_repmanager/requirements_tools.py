import logging
from pathlib import Path

import attr

log = logging.getLogger(__name__)


def _strip_version(raw: str) -> str:
    return raw.strip().split(" ;")[0]


@attr.s(frozen=True)
class PipRequirement:
    name: str = attr.ib()
    raw_version: str = attr.ib()

    @property
    def cleaned_version(self) -> str:
        return _strip_version(self.raw_version).removeprefix("==")


@attr.s(frozen=True)
class PipRequirementsIO:
    path: Path = attr.ib()

    def read_existing(self, gather_all: bool = False) -> dict[str, PipRequirement]:
        result = {}
        with open(self.path) as fh:
            for line in fh:
                try:
                    parts = line.strip().split("==")
                    name = parts[0].strip()
                    version = "==".join(parts[1:])
                    if " " in name or not name or name.startswith("#"):
                        # ignoring lines without actual requirements
                        continue
                    if gather_all or name not in ["mypy", "python"]:
                        result[name] = PipRequirement(name=name, raw_version=version)
                except Exception:
                    log.warning("Failed to parse %s from file %s", line, self.path)
        return result

    def write_updates(self, to_update: dict[str, PipRequirement]) -> None:
        requirements = self.read_existing(gather_all=True)
        requirements.update(to_update)
        with open(self.path, "w") as fh:
            fh.writelines(f"{name}=={requirement.raw_version}\n" for name, requirement in sorted(requirements.items()))
