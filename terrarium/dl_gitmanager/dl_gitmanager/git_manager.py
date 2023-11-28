from __future__ import annotations

from pathlib import Path
from typing import (
    Generator,
    Iterable,
    Optional,
)

import attr
from git.diff import Diff
from git.objects.commit import Commit
from git.objects.submodule.base import Submodule
from git.repo.base import Repo as GitRepo


MAX_HISTORY_DEPTH = 300


@attr.s
class GitManager:
    git_repo: GitRepo = attr.ib(kw_only=True)
    path_prefix: Path = attr.ib(kw_only=True, default=Path("."))

    def get_root_path(self) -> Path:
        return Path(self.git_repo.working_tree_dir)

    def get_commit_obj(self, commit_specifier: str) -> Commit:
        return self.git_repo.commit(commit_specifier)

    def iter_commits(self, base: str, head: str, only_missing_commits: bool) -> Iterable[Commit]:
        if only_missing_commits:
            return self.git_repo.iter_commits(f"{base}..{head}")
        else:
            return self.git_repo.iter_commits(f"{base}...{head}")

    def _iter_diffs_from_commit(self, commit_obj: Commit) -> Iterable[Diff]:
        for parent in commit_obj.parents:
            yield from commit_obj.diff(parent)

    def _get_submodule_commit(self, submodule: Submodule, commit_obj: Commit) -> str:
        tree_item = commit_obj.tree[submodule.name]
        return tree_item.hexsha

    def _iter_range_diffs(
        self,
        base: str,
        head: str,
        absolute: bool = False,
        submodules: bool = True,
        only_missing_commits: bool = False,
    ) -> Generator[tuple[Path, Diff], None, None]:
        # Get commit objects
        base_commit = self.get_commit_obj(base)
        head_commit = self.get_commit_obj(head)

        base_path = self.get_root_path() if absolute else self.path_prefix

        # Iter commits:
        for commit_obj in self.iter_commits(base=base, head=head, only_missing_commits=only_missing_commits):
            for diff_item in self._iter_diffs_from_commit(commit_obj):
                yield base_path, diff_item

        # Go to submodules if needed
        if submodules:
            # Iter submodules and get their internal diffs
            for submodule in self.git_repo.submodules:
                submodule_base = self._get_submodule_commit(submodule=submodule, commit_obj=base_commit)
                submodule_head = self._get_submodule_commit(submodule=submodule, commit_obj=head_commit)
                submodule_manager = self.get_submodule_manager(
                    submodule=submodule,
                    path_prefix=Path(submodule.path),
                )
                yield from submodule_manager._iter_range_diffs(
                    base=submodule_base,
                    head=submodule_head,
                    absolute=absolute,
                    only_missing_commits=only_missing_commits,
                    submodules=submodules,
                )

    def _collect_paths_from_diffs(self, diff_iterable: Iterable[tuple[Path, Diff]]) -> list[str]:
        result: set[str] = set()
        for base_path, diff_item in diff_iterable:
            if diff_item.a_path:
                result.add(str(base_path / diff_item.a_path))
            if diff_item.b_path:
                result.add(str(base_path / diff_item.b_path))

        return sorted(result)

    def get_range_diff_paths(
        self,
        base: str,
        head: str,
        absolute: bool = False,
        only_missing_commits: bool = False,
    ) -> list[str]:
        return self._collect_paths_from_diffs(
            diff_iterable=self._iter_range_diffs(
                base=base,
                head=head,
                absolute=absolute,
                only_missing_commits=only_missing_commits,
            )
        )

    def get_submodule_manager(self, submodule: Submodule, path_prefix: Optional[Path] = None) -> GitManager:
        return type(self)(git_repo=submodule.module(), path_prefix=path_prefix or Path("."))
