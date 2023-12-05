from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
import shutil
import tempfile
from typing import (
    Generator,
    Optional,
)
import uuid

import attr
import pytest

from dl_repmanager.fs_editor import FilesystemEditor
from dl_repmanager.package_index import (
    PackageIndex,
    PackageIndexBuilder,
)
from dl_repmanager.package_reference import PackageReference
from dl_repmanager.primitives import PackageInfo
from dl_repmanager.repository_env import (
    RepoEnvironment,
    RepoEnvironmentLoader,
)
from dl_repmanager.repository_manager import (
    PackageGenerator,
    RepositoryManager,
)
from dl_repmanager.repository_navigator import RepositoryNavigator
from dl_repmanager.toml_tools import (
    TOMLIOFactory,
    TOMLWriter,
)
from dl_repmanager_tests.unit.config import (
    DEFAULT_PACKAGE_TYPE,
    REPO_PATH,
)
from dl_repmanager_tests.unit.tool_runner import (
    PackageCliRunner,
    RepoCliRunner,
)


@attr.s
class Repo:
    """A container/initializer for a bunch of objects"""

    base_path: Path = attr.ib(kw_only=True)
    repository_env: RepoEnvironment = attr.ib(init=False)
    fs_editor: FilesystemEditor = attr.ib(init=False)
    package_index: PackageIndex = attr.ib(init=False)
    repository_navigator: RepositoryNavigator = attr.ib(init=False)
    package_generator: PackageGenerator = attr.ib(init=False)
    package_reference: PackageReference = attr.ib(init=False)
    repository_manager: RepositoryManager = attr.ib(init=False)

    @repository_env.default
    def _make_repository_env(self) -> RepoEnvironment:
        return RepoEnvironmentLoader().load_env(base_path=self.base_path)

    @fs_editor.default
    def _make_fs_editor(self) -> FilesystemEditor:
        return self.repository_env.get_fs_editor()

    @package_index.default
    def _make_package_index(self) -> PackageIndex:
        return PackageIndexBuilder(repository_env=self.repository_env).build_index()

    @repository_navigator.default
    def _make_repository_navigator(self) -> RepositoryNavigator:
        return RepositoryNavigator(repository_env=self.repository_env, package_index=self.package_index)

    @repository_manager.default
    def _make_repository_manager(self) -> RepositoryManager:
        return RepositoryManager(
            repository_env=self.repository_env,
            package_index=self.package_index,
            repository_navigator=self.repository_navigator,
            package_generator=PackageGenerator(repository_env=self.repository_env, package_index=self.package_index),
            package_reference=PackageReference(repository_env=self.repository_env, package_index=self.package_index),
        )

    def reload(self) -> Repo:
        return self.__class__(base_path=self.base_path)


@contextmanager
def deployed_repo(repo_src: Path) -> Generator[Path, None, None]:
    repo_dst = Path(tempfile.mkdtemp()) / repo_src.name
    shutil.copytree(repo_src, repo_dst)
    try:
        yield repo_dst
    finally:
        shutil.rmtree(repo_dst)


class RepmanagerTestingBase:
    @pytest.fixture(scope="class")
    def repo_path(self) -> Generator[Path, None, None]:
        with deployed_repo(REPO_PATH) as repo_path:
            yield repo_path

    @pytest.fixture(scope="class")
    def repo(self, repo_path: Path) -> Repo:
        return Repo(base_path=repo_path)

    def generate_package_name(self) -> str:
        return f"pkg_{uuid.uuid4().hex[:6]}"

    def init_package(
        self,
        repo: Repo,
        package_type: str = DEFAULT_PACKAGE_TYPE,
        package_name: Optional[str] = None,
    ) -> PackageInfo:
        package_name = package_name or self.generate_package_name()
        assert package_name is not None
        repo.repository_manager.init_package(package_type=package_type, package_module_name=package_name)
        repo = repo.reload()
        return repo.package_index.get_package_info_from_module_name(package_name)

    @pytest.fixture(scope="class")
    def package(self, repo: Repo) -> PackageInfo:
        return self.init_package(repo=repo)

    @contextmanager
    def toml_writer(self, repo: Repo, toml_path: Path) -> Generator[TOMLWriter, None, None]:
        toml_io_factory = TOMLIOFactory(fs_editor=repo.fs_editor)
        with toml_io_factory.toml_writer(toml_path) as toml_writer:
            yield toml_writer


class RepmanagerCliTestBase(RepmanagerTestingBase):
    def get_repo_cli(self, repo_path: Path) -> RepoCliRunner:
        return RepoCliRunner(repo_path=repo_path)

    def get_package_cli(self, package_path: Path) -> PackageCliRunner:
        return PackageCliRunner(package_path=package_path)

    @pytest.fixture(scope="class")
    def repo_cli(self, repo_path: Path) -> RepoCliRunner:
        return self.get_repo_cli(repo_path=repo_path)
