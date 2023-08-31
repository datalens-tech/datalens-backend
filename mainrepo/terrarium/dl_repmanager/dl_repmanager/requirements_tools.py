import logging
import sys
from pathlib import Path

import attr
import tomlkit

from dl_repmanager.exceptions import MypyStubsOutOfSyncError
from dl_repmanager.package_meta_reader import PackageMetaReader
from dl_repmanager.pypi_tools import get_package_info_by_version
from dl_repmanager.toml_tools import TOMLWriter

log = logging.getLogger(__name__)


def _strip_version(raw: str):
    return raw.strip().split(" ;")[0]


@attr.s(frozen=True)
class PipRequirement:
    name: str = attr.ib()
    raw_version: str = attr.ib()

    @property
    def cleaned_version(self):
        cleaned_version = _strip_version(self.raw_version)
        if cleaned_version.startswith("=="):
            cleaned_version = cleaned_version[2:]

        return cleaned_version


@attr.s(frozen=True)
class PipRequirementsIO:
    path: Path = attr.ib()

    def read_existing(self, gather_all: bool = False) -> dict[str, PipRequirement]:
        result = dict()
        with open(self.path) as fh:
            for line in fh.readlines():
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
                    log.warning(f"Failed to parse {line} from file {self.path}")
        return result

    def write_updates(self, to_update: dict[str, PipRequirement]):
        requirements = self.read_existing(gather_all=True)
        requirements.update(to_update)
        with open(self.path, "wt") as fh:
            for name, requirement in sorted(requirements.items()):
                fh.write(f"{name}=={requirement.raw_version}\n")
