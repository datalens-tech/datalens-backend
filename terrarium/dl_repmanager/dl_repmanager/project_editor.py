"""
Bunch of logic to edit pyproject.toml's across entire repo
"""

from functools import cached_property
from logging import getLogger
from pathlib import Path
import subprocess

import attr

from dl_repmanager.package_index import PackageIndex
from dl_repmanager.package_meta_reader import (
    PackageMetaIOFactory,
    PackageMetaReader,
)
from dl_repmanager.repository_env import RepoEnvironment


log = getLogger()


@attr.s
class PyPrjEditor:
    repository_env: RepoEnvironment = attr.ib(kw_only=True)
    package_index: PackageIndex = attr.ib(kw_only=True)

    @cached_property
    def base_path(self) -> Path:
        return self.repository_env.base_path

    @cached_property
    def package_meta_io_factory(self) -> PackageMetaIOFactory:
        return PackageMetaIOFactory(fs_editor=self.repository_env.fs_editor)

    def _get_meta_project_reader(self, metapackage_name: str) -> PackageMetaReader:
        toml_path = self.repository_env.get_metapackage_spec(metapackage_name=metapackage_name).toml_path
        with self.package_meta_io_factory.package_meta_reader(toml_path) as reader:
            return reader

    def update_mypy_common(self, metapackage_name: str) -> None:
        data = self._get_meta_project_reader(metapackage_name=metapackage_name).get_mypy_common()
        for pi in self.package_index.list_package_infos():
            with self.package_meta_io_factory.package_meta_writer(pi.toml_path) as pmw:
                log.debug(f"Checking {pi.abs_path}")
                pmw.update_mypy_common(data)

            log.debug(f"pyproject-fmt {str(pi.toml_path)}")
            subprocess.run(f"pyproject-fmt {str(pi.abs_path)}", shell=True)
            (pi.abs_path / "mypy.ini").unlink(missing_ok=True)
