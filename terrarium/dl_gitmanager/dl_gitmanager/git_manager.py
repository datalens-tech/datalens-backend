from __future__ import annotations

from pathlib import Path
from typing import (
    Collection,
    Generator,
    Iterable,
)

import attr
from git.diff import Diff
from git.repo.base import Repo as GitRepo


MAX_HISTORY_DEPTH = 300


@attr.s
class GitManager:
    git_repo: GitRepo = attr.ib(kw_only=True)

    def get_root_path(self) -> Path:
        return Path(self.git_repo.working_tree_dir)

    def get_head_commit(self) -> str:
        return self.git_repo.head.commit.hexsha

    def get_commit(self, commit_specifier: str) -> str:
        return self.git_repo.commit(commit_specifier).hexsha

    def _get_sm_commit(self, submodule_name: str, commit: str) -> str:
        return self.git_repo.commit(commit).tree[submodule_name].hexsha

    def _iter_range_diffs(
        self,
        base: str,
        head: str,
        absolute: bool = False,
        submodules: bool = True,
    ) -> Generator[tuple[Path, Diff], None, None]:
        base_commit = self.git_repo.commit(base)
        head_commit = self.git_repo.commit(head)

        base_path: Path
        if absolute:
            base_path = self.get_root_path()
        else:
            base_path = Path(".")

        # Iter own diffs
        diff_index = head_commit.diff(base_commit)
        for diff_item in diff_index:
            yield base_path, diff_item

        # Iter submodules and get their internal diffs
        for submodule in self.git_repo.submodules:
            submodule_name = submodule.name
            base_tree_item = base_commit.tree[submodule_name]
            head_tree_item = head_commit.tree[submodule_name]
            submodule_base = base_tree_item.hexsha
            submodule_head = head_tree_item.hexsha
            submodule_manager = self.get_submodule_manager(submodule_name=submodule_name)

            submodule_base_path: Path
            if absolute:
                submodule_base_path = submodule_manager.get_root_path()
            else:
                submodule_base_path = Path(submodule.path)

            # Iterate. Override the repo paths here with the submodule path
            for _, diff_item in submodule_manager._iter_range_diffs(base=submodule_base, head=submodule_head):
                yield submodule_base_path, diff_item

    def _iter_list_diffs(
        self, commits: Collection[str], absolute: bool = False
    ) -> Generator[tuple[Path, Diff], None, None]:
        if not commits:
            return

        base_path: Path
        if absolute:
            base_path = self.get_root_path()
        else:
            base_path = Path(".")

        # Iter own diffs
        for commit_str in commits:
            commit_obj = self.git_repo.commit(commit_str)
            for parent in commit_obj.parents:
                for diff_item in commit_obj.diff(parent):
                    yield base_path, diff_item

        # Iter submodules and get their internal diffs
        for submodule in self.git_repo.submodules:
            submodule_name = submodule.name
            submodule_manager = self.get_submodule_manager(submodule_name=submodule_name)
            sm_commits_for_all_commits: set[str] = set()
            for commit_str in commits:
                commit_obj = self.git_repo.commit(commit_str)
                submodule_commit = self._get_sm_commit(submodule_name=submodule_name, commit=commit_str)
                sm_ancestors = submodule_manager.get_all_ancestor_commits(submodule_commit)
                parent_commit_objs = commit_obj.parents
                for parent_commit_obj in parent_commit_objs:
                    sm_commit_of_parent = self._get_sm_commit(
                        submodule_name=submodule_name, commit=parent_commit_obj.hexsha
                    )
                    sm_ancestors -= submodule_manager.get_all_ancestor_commits(sm_commit_of_parent)

                sm_commits_for_all_commits |= sm_ancestors

            submodule_base_path: Path
            if absolute:
                submodule_base_path = submodule_manager.get_root_path()
            else:
                submodule_base_path = Path(submodule.path)

            for _, diff_item in submodule_manager._iter_list_diffs(commits=sm_commits_for_all_commits):
                yield submodule_base_path, diff_item

    def _collect_paths_from_diffs(self, diff_iterable: Iterable[tuple[Path, Diff]]) -> list[str]:
        result: set[str] = set()
        for base_path, diff_item in diff_iterable:
            if diff_item.a_path:
                result.add(str(base_path / diff_item.a_path))
            if diff_item.b_path:
                result.add(str(base_path / diff_item.b_path))

        return sorted(result)

    def get_range_diff_paths(self, base: str, head: str, absolute: bool = False) -> list[str]:
        return self._collect_paths_from_diffs(
            diff_iterable=self._iter_range_diffs(base=base, head=head, absolute=absolute)
        )

    def get_list_diff_paths(self, commits: Collection[str], absolute: bool = False) -> list[str]:
        return self._collect_paths_from_diffs(diff_iterable=self._iter_list_diffs(commits=commits, absolute=absolute))

    def get_all_ancestor_commits(self, commit: str) -> set[str]:
        commits = {commit.hexsha for commit in self.git_repo.iter_commits(commit, max_count=MAX_HISTORY_DEPTH)}
        return commits

    def get_missing_commits(self, base: str, head: str) -> set[str]:
        commits = {commit.hexsha for commit in self.git_repo.iter_commits(f"{base}...{head}")}
        return commits

    def get_submodule_manager(self, submodule_name: str) -> GitManager:
        submodule_dict = {sm.name: sm for sm in self.git_repo.submodules}
        submodule = submodule_dict[submodule_name]
        return type(self)(git_repo=submodule.module())
