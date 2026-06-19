"""
Bunch of logic to edit pyproject.toml's across entire repo
"""

from functools import cached_property
from pathlib import Path

import attr

from dl_repmanager.repository_env import RepoEnvironment


@attr.s
class PyPrjEditor:
    repository_env: RepoEnvironment = attr.ib(kw_only=True)

    @cached_property
    def base_path(self) -> Path:
        return self.repository_env.base_path
