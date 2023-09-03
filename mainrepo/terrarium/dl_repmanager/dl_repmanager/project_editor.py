"""
Bunch of logic to edit pyproject.toml's across entire repo
"""
import subprocess
from functools import cached_property
from logging import getLogger
from pathlib import Path

import attr

from dl_repmanager.env import RepoEnvironment
from dl_repmanager.package_index import PackageIndex
from dl_repmanager.package_meta_reader import PackageMetaReader, PackageMetaIOFactory

log = getLogger()


@attr.s
class PyPrjEditor:
    repo_env: RepoEnvironment = attr.ib(kw_only=True)
    package_index: PackageIndex = attr.ib(kw_only=True)

    @cached_property
    def base_path(self) -> Path:
        return self.repo_env.base_path

    @cached_property
    def package_meta_io_factory(self) -> PackageMetaIOFactory:
        return PackageMetaIOFactory(fs_editor=self.repo_env.fs_editor)

    @cached_property
    def meta_project_reader(self) -> PackageMetaReader:
        toml_path = self.base_path / 'ops/ci/pyproject.toml'
        with self.package_meta_io_factory.package_meta_reader(toml_path) as reader:
            return reader

    def update_mypy_common(self) -> None:
        data = self.meta_project_reader.get_mypy_common()
        for pi in self.package_index.list_package_infos():
            with self.package_meta_io_factory.package_meta_writer(pi.abs_path / "pyproject.toml") as pmw:
                log.debug(f"Checking {pi.abs_path}")
                pmw.update_mypy_common(data)

            log.debug(f"pyproject-fmt {str(pi.abs_path / 'pyproject.toml')}")
            subprocess.run(f"pyproject-fmt {str(pi.abs_path)}", shell=True)
            (pi.abs_path / 'mypy.ini').unlink(missing_ok=True)
