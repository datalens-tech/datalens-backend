import subprocess
from pathlib import Path
from typing import Optional

import attr


@attr.s()
class GitManager:
    _path: Path = attr.ib()
    _suppress_stdout_on_run = attr.ib(default=True)

    def _run_git(
            self,
            args: list[str],
            cwd: Optional[Path] = None,
            extract_stdout: bool = False,
    ) -> subprocess.CompletedProcess:
        actual_cwd: Path = cwd or self._path

        stdout_target: Optional[int]
        if extract_stdout:
            stdout_target = subprocess.PIPE
        elif self._suppress_stdout_on_run:
            stdout_target = subprocess.DEVNULL
        else:
            stdout_target = None

        return subprocess.run(
            ["git", *args],
            cwd=actual_cwd,
            stdout=stdout_target,
            check=True,
        )

    def _get_git_output(
            self,
            args: list[str],
            *,
            auto_strip: bool,
            cwd: Optional[Path] = None,
    ) -> str:
        proc = self._run_git(args=args, cwd=cwd, extract_stdout=True)
        stdout = proc.stdout.decode("ascii")
        if auto_strip:
            return stdout.strip("\n")
        return stdout

    def get_origin_url(self) -> str:
        return self._get_git_output(["config", "--get", "remote.origin.url"], auto_strip=True)

    def fetch(self) -> None:
        self._run_git(["fetch"])

    def copy_repo(self, destination: Path, fetch_if_exists: bool = False) -> "GitManager":
        if not destination.parent.exists():
            raise ValueError(f"Destination parent folder does not exists: {destination.parent}")

        current_origin_url = self.get_origin_url()

        dest_repo_manager = attr.evolve(self, path=destination)

        if destination.exists():
            dest_repo_origin_url = dest_repo_manager.get_origin_url()
            if dest_repo_origin_url != current_origin_url:
                raise ValueError("Copy target repo exists and it's origin does not match current.")

            if fetch_if_exists:
                dest_repo_manager.fetch()

        else:
            self._run_git(
                ["clone", current_origin_url, destination.name],
                cwd=destination.parent,
            )

        dest_repo_manager = attr.evolve(self, path=destination)

        return dest_repo_manager

    def checkout(self, rev: str) -> None:
        rev_hash = self._get_git_output(["rev-parse", rev], auto_strip=True)
        self._run_git(["checkout", rev_hash])
