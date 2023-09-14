import itertools
from pathlib import Path
import re
import shutil
import subprocess
from typing import Sequence

from dl_repmanager.metapkg_manager import MetaPackageManager
from dl_repmanager.primitives import (
    LocalReqPackageSpec,
    PypiReqPackageSpec,
)


def run_poetry_lock(dir_path: Path) -> None:
    subprocess.run(["poetry", "lock", "--no-update"], cwd=dir_path, check=True)


def sync_scoped_metapkg(
    *,
    original_metapkg_path: Path,
    scoped_metapkg_path: Path,
    package_dirs_to_include: Sequence[Path],  # Relative to scoped package
    prevent_prune_for_deps: Sequence[str],
    remove_private_pypi: bool = False,
    use_target_lock: bool = False,
):
    def is_local_package_in_scope(dep_from_scoped_pkg: LocalReqPackageSpec) -> bool:
        for package_dir in package_dirs_to_include:
            if dep_from_scoped_pkg.path.is_relative_to(package_dir):
                return True
        return False

    original_metapkg = MetaPackageManager(original_metapkg_path)
    scoped_metapkg = original_metapkg.as_new_location(scoped_metapkg_path)

    if not use_target_lock:
        shutil.copy(
            original_metapkg_path / "poetry.lock",
            scoped_metapkg_path / "poetry.lock",
        )

    scoped_metapkg.save()

    all_local_dependencies = scoped_metapkg.get_dependencies("ci")
    all_groups = scoped_metapkg.list_poetry_groups()
    declared_external_dependencies: list[PypiReqPackageSpec] = [
        dep for dep in scoped_metapkg.get_dependencies(None) if isinstance(dep, PypiReqPackageSpec)
    ]

    for dep in all_local_dependencies:
        if isinstance(dep, LocalReqPackageSpec) and not is_local_package_in_scope(dep):
            scoped_metapkg.remove_dependency("ci", dep)

    for group in all_groups:
        if group.startswith("app_") or "integration_tests" in group:
            scoped_metapkg.remove_poetry_group(group)

    scoped_metapkg.save()

    # Rerun lock
    run_poetry_lock(scoped_metapkg_path)

    ci_deps_raw = scoped_metapkg.export_dependencies_raw("ci")
    dev_deps_raw = scoped_metapkg.export_dependencies_raw("dev")

    effective_external_dependencies: set[str] = set()

    for line in itertools.chain(ci_deps_raw.splitlines(), dev_deps_raw.splitlines()):
        ext_dependency_re = re.compile(r"(^\S+)==\S+ ;")
        m = ext_dependency_re.match(line)

        if m:
            effective_external_dependencies.add(m.group(1).lower())

    for ext_dep in declared_external_dependencies:
        ext_dep_name = ext_dep.package_name.lower()

        if ext_dep_name not in effective_external_dependencies and ext_dep_name not in prevent_prune_for_deps:
            scoped_metapkg.remove_dependency(None, ext_dep.package_name)

    for mypy_override in scoped_metapkg.list_mypy_stubs_override():
        if mypy_override not in effective_external_dependencies and mypy_override not in prevent_prune_for_deps:
            scoped_metapkg.remove_mypy_stubs_override(mypy_override)

    scoped_metapkg.save()

    if remove_private_pypi:
        scoped_metapkg.remove_package_sources_section()
        scoped_metapkg.save()

    run_poetry_lock(scoped_metapkg_path)
