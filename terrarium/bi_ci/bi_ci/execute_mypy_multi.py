import asyncio
from asyncio import Queue
import enum
from functools import partial
import json
from pathlib import Path
import sys
from typing import Iterable

import attr
import clize
import tomlkit
from tomlkit.exceptions import NonExistentKey


PYPROJECT_TOML = "pyproject.toml"


@attr.s
class MypyRequest:
    pkg_dir: Path = attr.ib()
    targets: list[str] = attr.ib()


class MypyStatus(enum.Enum):
    SUCCESS = "success"
    FAIL = "fail"


@attr.s
class MypyResult:
    pkg_dir: Path = attr.ib()
    command: str = attr.ib()
    status: MypyStatus = attr.ib()
    stdout: bytes = attr.ib()
    stderr: bytes = attr.ib()


class CacheQueue:
    # control access to mypy cache directories
    # mypy should reuse fs cache as much as possible
    # but any concurrent runs mustn't access the same cache directory
    def __init__(self, base_mypy_cache_dir: Path, count: int):
        assert count > 0
        self._queue: Queue = Queue()
        for i in range(count):
            # use fixed names for cache folders for reusing the cache during local runs
            self._queue.put_nowait(
                base_mypy_cache_dir / Path(str(i)),
            )

    async def get(self) -> Path:
        return await self._queue.get()

    async def put(self, value: Path) -> None:
        await self._queue.put(value)


def get_mypy_targets(pkg_dir: Path) -> list[str]:
    with open(pkg_dir / PYPROJECT_TOML) as fh:
        meta = tomlkit.load(fh)
        try:
            return meta["datalens"]["meta"]["mypy"]["targets"]  # type: ignore  # 2024-01-30 # TODO: Value of type "Item | Container" is not indexable  [index]
        except NonExistentKey:
            print("No data in meta['datalens']['meta']['mypy']['targets']")
        try:
            # just run mypy only on application/lib code
            # we don't run mypy on tests for backward compatability because there are a lot of mypy issues already
            targets: list[str] = [
                package["include"]  # type: ignore # 2024-03-08 # Invalid index type "str" for "Any | str"; expected type "SupportsIndex | slice"  [index]
                for package in meta["tool"]["poetry"]["packages"]  # type: ignore  # 2024-03-08 # TODO: Value of type "Item | Container" is not indexable  [index]
            ]
            return targets
        except NonExistentKey:
            print("No data in meta['tools']['poetry']['packages']")

    # fallback
    dirname = pkg_dir.name or ""
    return [dirname]


def get_python_packages(root: Path) -> Iterable[str]:
    for path in root.rglob(f"*{PYPROJECT_TOML}"):
        yield str(path.parent).replace(PYPROJECT_TOML, "")


async def run_mypy(request: MypyRequest, cache_queue: CacheQueue) -> MypyResult:
    mypy_cache_dir = await cache_queue.get()
    mypy_cache_dir.mkdir(exist_ok=True)

    command_args = ["mypy", f"--cache-dir={mypy_cache_dir}", *request.targets]
    command = " ".join(command_args)
    proc = await asyncio.create_subprocess_shell(
        command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=str(request.pkg_dir),
    )
    stdout, stderr = await proc.communicate()
    await cache_queue.put(mypy_cache_dir)
    return MypyResult(
        pkg_dir=request.pkg_dir,
        command=command,
        status=MypyStatus.SUCCESS if proc.returncode == 0 else MypyStatus.FAIL,
        stdout=stdout,
        stderr=stderr,
    )


def main(root: Path, targets_file: Path = None, processes: int = 1) -> None:  # type: ignore  # 2024-01-30 # TODO: Incompatible default for argument "targets_file" (default has type "None", argument has type "Path")  [assignment]
    """
    Run mypy on the datalens backend code.
    We have to run mypy separately on each project
    because mypy reads its settings from pyproject.toml from its current working directory.

    This script DON'T check mypy in tests.
    """
    # clize can't recognize type annotation "Optional"
    package_rel_paths: Iterable[str]
    if targets_file is not None:
        package_rel_paths = json.load(open(targets_file))
    else:
        package_rel_paths = get_python_packages(root)

    base_mypy_cache_dir = Path("/tmp/mypy_cache")
    base_mypy_cache_dir.mkdir(exist_ok=True)

    mypy_requests: list[MypyRequest] = []
    for rel_path in package_rel_paths:
        print()  # just separator
        pkg_dir = root / rel_path
        targets = get_mypy_targets(pkg_dir)
        if len(targets) == 0:
            print(f"SKIP: there weren't any valid targets for {pkg_dir}")
            continue
        print(f"PLAN: process {pkg_dir} with {targets}")
        mypy_requests.append(
            MypyRequest(
                pkg_dir=pkg_dir,
                targets=targets,
            )
        )

    cache_queue = CacheQueue(
        base_mypy_cache_dir=base_mypy_cache_dir,
        count=processes,
    )
    loop = asyncio.get_event_loop()
    results: Iterable[MypyResult] = loop.run_until_complete(
        asyncio.gather(*[run_mypy(request=request, cache_queue=cache_queue) for request in mypy_requests])
    )

    print("\nRESULT\n")
    bad_pkg_dirs: list[str] = []
    for result in results:
        print(f"PACKAGE: {result.pkg_dir}")
        print(f"CMD: {result.command}")
        print(f"{result.stdout.decode('utf-8')}")
        print(f"{result.stderr.decode('utf-8')}")
        if result.status == MypyStatus.FAIL:
            bad_pkg_dirs.append(str(result.pkg_dir))

    if bad_pkg_dirs:
        print("\nFAIL: mypy failed:")
        print("\n".join(sorted(bad_pkg_dirs)))

    sys.exit(1 if bad_pkg_dirs else 0)


cmd = partial(clize.run, main)

if __name__ == "__main__":
    cmd()
