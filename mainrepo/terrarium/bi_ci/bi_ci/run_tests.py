import uuid
from functools import partial
import subprocess
from pathlib import Path
import sys

import attr
import clize
import tomlkit
from tomlkit import TOMLDocument


@attr.s
class Target:
    name: str = attr.ib()
    # where to run pytest
    root_dir: Path | None = attr.ib(default=None)
    # what should be pytest argument, relative to root_dir
    target_path: str | None = attr.ib(default=None)


def read_pkg(path: Path) -> TOMLDocument:
    return tomlkit.load(open(path))


def pkg_to_targets_ref(pkg_dir: Path, doc: TOMLDocument, sections: list[str] | None = None) -> list[Target]:
    result = list()
    for k, v in doc.get("datalens", {}).get("pytest", {}).items():
        if sections:
            if k not in sections:
                continue

        target_path = v.get("target_path", None)
        if target_path:
            for tp in target_path.split(" "):
                result.append(
                    Target(
                        name=k,
                        root_dir=pkg_dir / v.get("root_dir", None),
                        target_path=tp,
                    )
                )
        else:
            result.append(
                Target(
                    name=k,
                    root_dir=pkg_dir / v.get("root_dir", None),
                    target_path=v.get("target_path", None),
                )
            )

    return result


def run_pytest_one(t: Target) -> int:
    print(f"Running pytest for {t.name} in {str(t.root_dir)} for {t.target_path or '.'}")
    report_name = f"{str(t.root_dir)}-{t.target_path or '.'}-{uuid.uuid4().hex}.xml".replace("/", "_")
    run_args = [
        "/venv/bin/python",
        "-m",
        "pytest",
        "-v",
        "--disable-warnings",  # todo: remove after migration from arc is finished
        f"--junitxml=/report/{report_name}",
    ]
    if t.target_path:
        run_args.append(t.target_path)

    subprocess_kwargs = dict(
        universal_newlines=True,
    )
    if t.root_dir:
        subprocess_kwargs["cwd"] = str(t.root_dir.expanduser().resolve())
    print(f"{run_args=}")
    print(f"{subprocess_kwargs=}")

    process = subprocess.run(run_args, **subprocess_kwargs)
    return process.returncode


def runner(test_target: str):
    """
    :param test_target: semicolon separated pair of package local path and target test section
    e.g: "lib/bi_core:unit_aio_api_commons"

    :return:
    """
    # todo: consider multiple targets for one pkg_root, maybe comma separated

    pkg_root, target_section = test_target.split(":")
    pkg_root = Path("/data") / pkg_root

    print("Datalens gh ci tests Runner welcomes you!")
    doc = read_pkg(pkg_root / "pyproject.toml")
    if target_section == "__default__":
        targets = []
    else:
        targets = pkg_to_targets_ref(pkg_root, doc, [target_section])

    if len(targets) == 0:
        print(f"Did not find specific instructions for running tests in {pkg_root} fallback to the simple pytest call")
        target = Target("__main__", pkg_root, ".")
        sys.exit(run_pytest_one(target))

    # Later we would probably filter this list depending on additional marks for
    #   for test targets and ENV vars defining which kind of tests should be executed
    # E.g. short vs long-running, unit vs external

    exit_code = 0
    for t in targets:
        run_exit_code = run_pytest_one(t)
        if run_exit_code not in [0, 5]:
            exit_code = 1

    sys.exit(exit_code)


runner_cli = partial(clize.run, runner)
