import re
import shutil
import subprocess
from pathlib import Path
from typing import Sequence

from dl_repmanager.metapkg_manager import MetaPackageManager
from dl_repmanager.primitives import LocalReqPackageSpec, PypiReqPackageSpec


def run_poetry_lock(dir_path: Path) -> None:
    subprocess.run(["poetry", "lock", "--no-update"], cwd=dir_path, check=True)


def sync_scoped_metapkg(
        *,
        original_metapkg_path: Path,
        scoped_metapkg_path: Path,
        package_dirs_to_include: Sequence[Path],  # Relative to scoped package
        preserve_lock: bool = False,
):
    def is_local_package_in_scope(dep_from_scoped_pkg: LocalReqPackageSpec) -> bool:
        for package_dir in package_dirs_to_include:
            if dep_from_scoped_pkg.path.is_relative_to(package_dir):
                return True
        return False

    original_metapkg = MetaPackageManager(original_metapkg_path)
    scoped_metapkg = original_metapkg.as_new_location(scoped_metapkg_path)

    if not preserve_lock:
        shutil.copy(
            original_metapkg_path / "poetry.lock",
            scoped_metapkg_path / "poetry.lock",
        )

    scoped_metapkg.save()

    all_local_dependencies = scoped_metapkg.get_dependencies("ci")
    all_groups = scoped_metapkg.list_poetry_groups()
    declared_external_dependencies: list[PypiReqPackageSpec] = [
        dep for dep in scoped_metapkg.get_dependencies(None)
        if isinstance(dep, PypiReqPackageSpec)
    ]

    for dep in all_local_dependencies:
        if isinstance(dep, LocalReqPackageSpec) and not is_local_package_in_scope(dep):
            scoped_metapkg.remove_dependency("ci", dep)

    for group in all_groups:
        if group.startswith("app_") or "integration_tests" in group:
            scoped_metapkg.remove_poetry_group(group)

    scoped_metapkg.save()

    # Rerun lock 
    if not preserve_lock:
        run_poetry_lock(scoped_metapkg_path)

    poetry_export = subprocess.run(
        ["poetry", "export", "--only", "ci", "--without-hashes"],
        stdout=subprocess.PIPE,
        stderr=None,
        cwd=scoped_metapkg_path,
        check=True,
    )
    effective_external_dependencies: set[str] = set()

    for line in poetry_export.stdout.decode("ascii").split("\n"):
        ext_dependency_re = re.compile(r"(^\S+)==\S+ ;")
        m = ext_dependency_re.match(line)

        if m:
            effective_external_dependencies.add(m.group(1).lower())

    for ext_dep in declared_external_dependencies:
        if ext_dep.package_name.lower() not in effective_external_dependencies:
            scoped_metapkg.remove_dependency(None, ext_dep.package_name)

    scoped_metapkg.save()

    if not preserve_lock:
        run_poetry_lock(scoped_metapkg_path)
