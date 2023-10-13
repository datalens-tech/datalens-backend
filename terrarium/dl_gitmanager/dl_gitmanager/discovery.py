from pathlib import Path

from git.exc import InvalidGitRepositoryError
from git.repo.base import Repo as GitRepo


def discover_repo(repo_path: Path) -> GitRepo:
    git_repo: GitRepo
    while True:
        if repo_path.parent == repo_path or not repo_path.exists():
            raise RuntimeError("Failed to find git repository")

        try:
            git_repo = GitRepo(path=repo_path)
            break
        except InvalidGitRepositoryError:
            repo_path = repo_path.parent

    return git_repo
