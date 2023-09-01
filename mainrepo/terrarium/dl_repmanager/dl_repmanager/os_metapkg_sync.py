import os
import re
import shutil
import subprocess

from dl_repmanager.metapkg_manager import MetaPackageManager
from dl_repmanager.primitives import LocalReqPackageSpec, PypiReqPackageSpec

ROOT_LOCATION = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../.."))

PUBLIC_METAPKG_LOCATION = os.path.join(ROOT_LOCATION, "mainrepo/metapkg")
PRIVATE_METAPKG_LOCATION = os.path.join(ROOT_LOCATION, "ops/ci")


def get_private_metapkg_mgr() -> MetaPackageManager:
    return MetaPackageManager(PRIVATE_METAPKG_LOCATION)


def run_poetry_lock(dir_path: str) -> None:
    subprocess.run(["poetry", "lock", "--no-update"], cwd=dir_path, check=True)


def main(
        preserve_lock: bool = False,
):
    private_metapkg = get_private_metapkg_mgr()
    public_metapkg = private_metapkg.as_new_location(PUBLIC_METAPKG_LOCATION)

    if not preserve_lock:
        shutil.copy(
            os.path.join(PRIVATE_METAPKG_LOCATION, "poetry.lock"),
            os.path.join(PUBLIC_METAPKG_LOCATION, "poetry.lock"),
        )

    public_metapkg.save()

    all_local_dependencies = public_metapkg.get_dependencies("ci")
    all_groups = public_metapkg.list_poetry_groups()
    declared_external_dependencies: list[PypiReqPackageSpec] = [
        dep for dep in public_metapkg.get_dependencies(None)
        if isinstance(dep, PypiReqPackageSpec)
    ]

    for dep in all_local_dependencies:
        if isinstance(dep, LocalReqPackageSpec) and not dep.path.startswith("../lib"):
            public_metapkg.remove_dependency("ci", dep)

    for group in all_groups:
        if group.startswith("app_") or "integration_tests" in group:
            public_metapkg.remove_poetry_group(group)

    public_metapkg.save()

    # Rerun lock 
    if not preserve_lock:
        run_poetry_lock(PUBLIC_METAPKG_LOCATION)

    poetry_export = subprocess.run(
        ["poetry", "export", "--only", "ci", "--without-hashes"],
        stdout=subprocess.PIPE,
        stderr=None,
        cwd=PUBLIC_METAPKG_LOCATION,
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
            public_metapkg.remove_dependency(None, ext_dep.package_name)

    public_metapkg.save()

    if not preserve_lock:
        run_poetry_lock(PUBLIC_METAPKG_LOCATION)


if __name__ == '__main__':
    main()
