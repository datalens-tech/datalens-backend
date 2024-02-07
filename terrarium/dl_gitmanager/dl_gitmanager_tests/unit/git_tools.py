from __future__ import annotations

from functools import singledispatchmethod
import os
from pathlib import Path
import uuid

import attr
from git.repo.base import Repo as GitRepo


@attr.s(frozen=True)
class GitAction:
    pass

    def get_paths(self) -> frozenset[Path]:
        return frozenset()


@attr.s(frozen=True)
class MultiGitAction(GitAction):
    actions: tuple[GitAction, ...] = attr.ib(kw_only=True, default=())

    def get_paths(self) -> frozenset[Path]:
        return frozenset(path for sub_action in self.actions for path in sub_action.get_paths())


@attr.s(frozen=True)
class GitFileAction(GitAction):
    path: Path = attr.ib(kw_only=True)

    def get_paths(self) -> frozenset[Path]:
        return frozenset((self.path,))


@attr.s(frozen=True)
class AddGitFileAction(GitFileAction):
    pass


@attr.s(frozen=True)
class RemoveGitFileAction(GitFileAction):
    pass


@attr.s(frozen=True)
class UpdateGitFileAction(GitFileAction):
    pass


@attr.s(frozen=True)
class SubmoduleSpec:
    name: str = attr.ib(kw_only=True)
    path: Path = attr.ib(kw_only=True)
    url: str = attr.ib(kw_only=True)


@attr.s(frozen=True)
class GitSubmoduleAction(GitAction):
    submodule: SubmoduleSpec = attr.ib(kw_only=True)

    def get_paths(self) -> frozenset[Path]:
        return frozenset((self.submodule.path,))


@attr.s(frozen=True)
class AddGitSubmoduleAction(GitSubmoduleAction):
    pass


@attr.s(frozen=True)
class UpdateGitSubmoduleAction(GitSubmoduleAction):
    new_commit: str = attr.ib(kw_only=True)


@attr.s
class GitActionProcessor:
    _repo_path: Path = attr.ib(kw_only=True)
    _git_repo: GitRepo = attr.ib(init=False)

    @_git_repo.default
    def _make_git_repo(self) -> GitRepo:
        return GitRepo(path=self._repo_path)

    @property
    def git_repo(self) -> GitRepo:
        return self._git_repo

    def generate_path(self) -> Path:
        return self._repo_path / uuid.uuid4().hex

    @singledispatchmethod
    def _process_action(self, action: GitAction) -> None:
        raise TypeError(f"Unsupported action type: {type(action)}")

    @_process_action.register
    def _process_multi_action(self, action: MultiGitAction) -> None:
        for git_action in action.actions:
            self._process_action(git_action)

    @_process_action.register
    def _process_add_file_action(self, action: AddGitFileAction) -> None:
        assert not action.path.exists()
        with open(action.path, mode="w") as f:
            f.write(str(os.urandom(1024)))
        self._git_repo.index.add(str(action.path))

    @_process_action.register
    def _process_remove_file_action(self, action: RemoveGitFileAction) -> None:
        assert action.path.exists()
        self._git_repo.index.remove(str(action.path))
        os.remove(action.path)

    @_process_action.register
    def _process_update_file_action(self, action: UpdateGitFileAction) -> None:
        assert action.path.exists()
        with open(action.path, mode="r+") as f:
            f.write(str(os.urandom(1024)))
        self._git_repo.index.add(str(action.path))

    @_process_action.register
    def _process_add_submodule_action(self, action: GitSubmoduleAction) -> None:
        sm_path = action.submodule.path
        assert not sm_path.exists()
        sm_obj = self._git_repo.create_submodule(name=action.submodule.name, url=action.submodule.url, path=sm_path)
        sm_obj.update(init=True)
        self._git_repo.index.add(str(sm_path))

    @_process_action.register
    def _process_update_submodule_action(self, action: UpdateGitSubmoduleAction) -> None:
        sm_path = action.submodule.path
        assert sm_path.exists()
        sm_obj = self._git_repo.submodule(action.submodule.name)
        sm_obj.module().git.checkout(action.new_commit)
        self._git_repo.index.add(str(sm_path))

    def checkout_new_branch(self, branch_name: str) -> None:
        new_branch = self._git_repo.create_head(branch_name)
        new_branch.checkout()

    def checkout_existing_branch(self, branch_name: str) -> None:
        branch = getattr(self._git_repo.heads, branch_name)
        branch.checkout()

    def add_commit(self, message: str, action: GitAction = MultiGitAction()) -> str:  # noqa: B008
        self._process_action(action)
        self._git_repo.index.commit(message)
        return self._git_repo.commit().hexsha

    @classmethod
    def initialize_repo(cls, repo_path: Path) -> GitActionProcessor:
        GitRepo.init(repo_path)
        return cls(repo_path=repo_path)

    def get_submodule_proc(self, submodule: SubmoduleSpec) -> GitActionProcessor:
        return GitActionProcessor(repo_path=submodule.path)

    def revert_submodules(self) -> None:
        for sm_obj in self._git_repo.submodules:
            sm_obj.update(init=True)

    def checkout_commit(self, commit: str) -> None:
        self._git_repo.git.checkout(commit)

    def get_current_commit(self) -> str:
        return self._git_repo.commit().hexsha
