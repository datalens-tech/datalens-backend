from pathlib import Path
import shutil
import tempfile
from typing import Generator

from git.repo.base import Repo as GitRepo
import pytest

from dl_gitmanager.git_manager import GitManager
from dl_gitmanager_tests.unit.git_tools import GitActionProcessor


@pytest.fixture(scope="function")
def base_repo_dir() -> Generator[Path, None, None]:
    dir_path = Path(tempfile.mkdtemp())
    try:
        yield dir_path
    finally:
        shutil.rmtree(dir_path)


@pytest.fixture(scope="function")
def git_action_proc(base_repo_dir: Path) -> GitActionProcessor:
    git_action_proc = GitActionProcessor.initialize_repo(base_repo_dir)
    git_action_proc.add_commit(message="Initial commit")
    git_action_proc.checkout_new_branch("main")
    return git_action_proc


@pytest.fixture(scope="function")
def git_manager(base_repo_dir: Path) -> GitManager:
    git_manager = GitManager(git_repo=GitRepo(path=base_repo_dir))
    return git_manager
