import logging
from pathlib import Path

import attr

from dl_repmanager.exceptions import MypyStubsOutOfSyncError
from dl_repmanager.package_meta_reader import (
    PackageMetaReader,
    PackageMetaWriter,
)
from dl_repmanager.pypi_tools import get_package_info_by_version
from dl_repmanager.requirements_tools import (
    PipRequirement,
    PipRequirementsIO,
)

log = logging.getLogger(__name__)


def _strip_version(raw: str) -> str:
    return raw.strip().split(" ;")[0]


def mb_types_name(name: str, override: dict | None = None) -> str | None:
    guessed_name = f"types-{name}"
    match override:
        case {"ignore": True}:
            return None
        case {"name": name}:
            return name
        case _:
            return guessed_name


def process_one(requirement: PipRequirement, override: dict | None = None) -> tuple[str, str] | None:
    guessed_name = f"types-{requirement.name}"
    cleaned_version = requirement.cleaned_version

    log.debug("%s override: %s", requirement.name, override)
    match override:
        case {"name": override_name, "version": override_version}:
            return override_name, override_version
        case {"version": override_version}:
            if check_if_exists_in_pypi(guessed_name, override_version):
                return guessed_name, override_version
            log.error("PyPI doesn't have package %s with version from overrides: %s", guessed_name, override_version)
            return None
        case {"name": override_name}:
            if check_if_exists_in_pypi(override_name, cleaned_version):
                return override_name, cleaned_version
            log.debug("PyPI doesn't have package %s with name from overrides: %s", guessed_name, override_name)
            return None
        case {"ignore": True}:
            log.debug("Skip check for types- pkg for %s due to override", requirement.name)
            return None
        case _:
            log.debug("No override for %s", requirement.name)

    if check_if_exists_in_pypi(guessed_name, cleaned_version):
        return guessed_name, cleaned_version

    return None


def check_if_exists_in_pypi(name: str, version: str) -> bool:
    log.debug("Trying to check presence of %s==%s on PyPI", name, version)
    data = get_package_info_by_version(name, version)
    if data.get("message") == "Not Found" or "info" not in data:
        log.info("Stub files package %s:%s is not available on PyPI", name, version)
        return False

    if "info" in data and not data["info"].get("yanked") and data["info"].get("name", "").lower() == name:
        log.debug("Stub files exists: %s version %s", name, version)
        return True

    log.info("Stub files package %s:%s is not available on PyPI", name, version)
    return False


@attr.s(frozen=True)
class RequirementsPathProvider:
    base_path: Path = attr.ib()
    external_requirements_rel_py: Path = attr.ib(default=Path("ops/ci/docker_image_base_ci"))
    external_requirements_file_names: tuple[str, ...] = attr.ib(default=("requirements_external.txt",))
    mypy_requirements_rel_path: Path = attr.ib(default=Path("ops/ci/docker_image_ci_mypy/requirements_types.txt"))

    def get_mypy_path(self) -> Path:
        return self.base_path / self.mypy_requirements_rel_path

    def get_external_requirements_path_list(self) -> list[Path]:
        return [
            self.base_path / self.external_requirements_rel_py / name for name in self.external_requirements_file_names
        ]


def stubs_sync(
    meta_reader: PackageMetaReader,
    meta_writer: PackageMetaWriter,
    path_provider: RequirementsPathProvider,
    dry_run: bool = True,
) -> None:
    log.info("Starting mypy stubs sync. Dry run: %s", dry_run)

    requirements: dict[str, PipRequirement] = {}
    for path in path_provider.get_external_requirements_path_list():
        requirements.update(PipRequirementsIO(path).read_existing())

    annotations_io = PipRequirementsIO(path_provider.get_mypy_path())
    annotations_requirements = annotations_io.read_existing()

    overrides_map = meta_reader.get_mypy_stubs_overrides()
    ignore_pkg_set = {k for k, v in overrides_map.items() if "ignore" in v}

    packages_to_ignore: list[str] = []
    annotations_to_add: dict[str, PipRequirement] = {}

    for name, req in sorted(requirements.items()):
        override = overrides_map.get(name)
        result = process_one(req, override)
        if result:
            types_name, types_version = result
            existing_requirement = annotations_requirements.get(types_name)
            if existing_requirement is None or existing_requirement.cleaned_version != types_version:
                annotations_to_add[types_name] = PipRequirement(name=name, raw_version=types_version)

        else:
            log.debug("Could not find a types pkg for %s", name)
            mb_name = mb_types_name(name, override)
            if mb_name and not annotations_requirements.get(mb_name) and name not in overrides_map:
                packages_to_ignore.append(name)

            if mb_name and annotations_requirements.get(mb_name) and name not in ignore_pkg_set:
                log.warning(
                    "%s and types package %s listed in requirements. Pypi does not have such packages", name, mb_name
                )

    if len(packages_to_ignore) + len(annotations_to_add) > 0:
        log.info("Need to add (or replace) in requirement_types.txt %s records", len(annotations_to_add))
        log.info("annotations_to_add=%r", annotations_to_add)
        log.info("Need to add ignores to the pyproject.toml: %s records", len(packages_to_ignore))
        log.info("packages_to_ignore=%r", packages_to_ignore)

        if not dry_run:
            annotations_io.write_updates(annotations_to_add)
            meta_writer.add_mypy_overrides_ignore(packages_to_ignore)
            log.info("Fixes done")
        else:
            raise MypyStubsOutOfSyncError(
                "At least some package missing stub packages, version is outdated or"
                " ignore mark should be added to the pyproject toml"
            )
    else:
        log.info("No changes required")
