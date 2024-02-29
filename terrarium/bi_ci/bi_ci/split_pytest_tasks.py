import dataclasses
import functools
import json
import pathlib
import sys
import typing

import clize
import tomlkit
import typing_extensions


"""
example test spec:

[datalens.pytest.unit]
root_dir = "dl_core_tests/"
target_path = "unit"

[datalens.pytest.db_part_1]
root_dir = "dl_core_tests/db/"
target_path = "aio caches capabilities common"
labels = ["fat"]
"""

DEFAULT_MODE = "base"
DEFAULT_LABELS = frozenset([DEFAULT_MODE])
DEFAULT_TARGET_NAME = "__default__"
DEFAULT_TARGET_PATHS = frozenset(["."])


@dataclasses.dataclass
class TestTarget:
    relative_path: str
    section: str
    requested_mode: str


@dataclasses.dataclass
class PyprojectPytestTarget:
    root_dir: str
    target_paths: frozenset[str] = DEFAULT_TARGET_PATHS
    name: str = DEFAULT_TARGET_NAME
    labels: frozenset[str] = DEFAULT_LABELS

    @classmethod
    def from_raw(cls, name: str, data: dict[str, typing.Any]) -> typing_extensions.Self:
        if "target_path" in data:
            target_paths = frozenset(str(data["target_path"]).split(" "))
        else:
            target_paths = DEFAULT_TARGET_PATHS

        if "labels" in data:
            labels = frozenset(data["labels"])
        else:
            labels = DEFAULT_LABELS

        return cls(
            name=name,
            root_dir=data["root_dir"],
            target_paths=target_paths,
            labels=labels,
        )


def format_output(name: str, targets: typing.Iterable[TestTarget]) -> str:
    data = [f"{target.relative_path}:{target.section}" for target in targets]

    return f"{name}={json.dumps(data)}"


def print_output(requested_modes: set[str], test_targets: typing.Iterable[TestTarget]) -> None:
    targets_by_mode: dict[str, list] = {}
    for target in test_targets:
        targets_by_mode.setdefault(target.requested_mode, []).append(target)

    for requested_mode in requested_modes:
        targets = targets_by_mode.get(requested_mode, [])
        formatted_output = format_output(f"split_{requested_mode}", targets)
        print(formatted_output)


def print_error(message: str) -> None:
    print(message, file=sys.stderr)


def read_package_paths(
    targets_path: pathlib.Path,
) -> typing.Generator[str, None, None]:
    with open(targets_path, "r") as file:
        data = file.read()
        data = data.strip()
        data = data.replace("'", '"')

    for item in json.loads(data):
        package_relative_path = item.strip()
        yield package_relative_path


def read_pytest_targets(absolute_path: pathlib.Path, relative_path: str) -> list[PyprojectPytestTarget]:
    pyproject_path = absolute_path / "pyproject.toml"

    if not pyproject_path.is_file():
        error_message = f"pyproject.toml not found in {relative_path}"
        print_error(error_message)
        raise FileNotFoundError(error_message)

    with open(pyproject_path, "r") as file:
        toml_data = tomlkit.parse(file.read())

    raw_targets = toml_data.get("datalens", {}).get("pytest", {})

    if len(raw_targets) == 0:
        return [PyprojectPytestTarget(root_dir=f"{absolute_path.name}_tests/")]

    return [PyprojectPytestTarget.from_raw(name=name, data=data) for name, data in raw_targets.items()]


def get_target_tests(
    relative_path: str,
    requested_labels: set[str],
    target: PyprojectPytestTarget,
    raise_on_unused_label: bool = False,
) -> typing.Generator[TestTarget, None, None]:
    requested_labels = requested_labels & target.labels
    unused_labels = target.labels - requested_labels

    if len(unused_labels) > 0:
        error_message = f"Unused labels in {relative_path}:{target.name}: {' '.join(unused_labels)}"
        print_error(error_message)

        if raise_on_unused_label:
            raise ValueError(error_message)

    for requested_mode in requested_labels:
        yield TestTarget(relative_path=relative_path, section=target.name, requested_mode=requested_mode)


def is_parent_path(parent: pathlib.Path, child: pathlib.Path) -> bool:
    return parent in child.parents


def get_tests_root_dirs(
    package_path: pathlib.Path,
    pytest_targets: list[PyprojectPytestTarget],
) -> set[pathlib.Path]:
    all_root_dirs: set[pathlib.Path] = set()

    # adding default test diirectory
    all_root_dirs.add(package_path / f"{package_path.name}_tests")

    for target in pytest_targets:
        root_dir = package_path / target.root_dir
        all_root_dirs.add(root_dir)

        if not root_dir.is_dir():
            print_error(f"Root dir {root_dir.name} not found in {root_dir.parent}")

    # deduplicating subdirectories

    root_dirs: set[pathlib.Path] = set()
    for root_dir in all_root_dirs:
        for other_root_dir in all_root_dirs:
            if is_parent_path(parent=other_root_dir, child=root_dir) and other_root_dir != root_dir:
                break
        else:
            root_dirs.add(root_dir)

    return root_dirs


def get_tests_covered_dirs(
    absolute_path: pathlib.Path,
    pytest_targets: list[PyprojectPytestTarget],
) -> set[pathlib.Path]:
    covered_dirs: set[pathlib.Path] = set()
    for target in pytest_targets:
        root_dir = absolute_path / target.root_dir

        for target_path in target.target_paths:
            absolute_target_path = root_dir / target_path
            covered_dirs.add(absolute_target_path)

            if not absolute_target_path.is_dir():
                print_error(f"Target dir {target_path} not found in {root_dir}")

    return covered_dirs


def iterate_over_pytest_files(path: pathlib.Path) -> typing.Generator[pathlib.Path, None, None]:
    if not path.is_dir():
        return

    for file in path.iterdir():
        if file.is_dir():
            yield from iterate_over_pytest_files(file)
        elif file.suffix == ".py" and file.name.startswith("test_"):
            yield file


def validate_test_coverage(
    absolute_path: pathlib.Path,
    relative_path: str,
    pytest_targets: list[PyprojectPytestTarget],
    raise_on_uncovered_test: bool = False,
) -> None:
    root_dirs = get_tests_root_dirs(package_path=absolute_path, pytest_targets=pytest_targets)
    covered_dirs = get_tests_covered_dirs(absolute_path=absolute_path, pytest_targets=pytest_targets)

    for root_dir_path in root_dirs:
        for file in iterate_over_pytest_files(root_dir_path):
            for covered_dir in covered_dirs:
                if is_parent_path(parent=covered_dir, child=file):
                    break
            else:
                error_message = f"Uncovered test file {file} found in {relative_path}"
                print_error(error_message)

                if raise_on_uncovered_test:
                    raise ValueError(error_message)


def get_package_tests(
    absolute_path: pathlib.Path,
    relative_path: str,
    requested_labels: set[str],
    raise_on_unused_label: bool = False,
    raise_on_uncovered_test: bool = False,
) -> typing.Generator[TestTarget, None, None]:
    try:
        pytest_targets = read_pytest_targets(absolute_path=absolute_path, relative_path=relative_path)
    except FileNotFoundError:
        return

    validate_test_coverage(
        absolute_path=absolute_path,
        relative_path=relative_path,
        pytest_targets=pytest_targets,
        raise_on_uncovered_test=raise_on_uncovered_test,
    )

    for target in pytest_targets:
        yield from get_target_tests(
            relative_path=relative_path,
            requested_labels=requested_labels,
            target=target,
            raise_on_unused_label=raise_on_unused_label,
        )


def get_tests(
    requested_labels: set[str],
    root_dir: pathlib.Path,
    test_targets_json_path: pathlib.Path,
    raise_on_unused_label: bool = False,
    raise_on_uncovered_test: bool = False,
) -> typing.Generator[TestTarget, None, None]:
    for package_relative_path in read_package_paths(targets_path=test_targets_json_path):
        yield from get_package_tests(
            absolute_path=root_dir / package_relative_path,
            relative_path=package_relative_path,
            requested_labels=requested_labels,
            raise_on_unused_label=raise_on_unused_label,
            raise_on_uncovered_test=raise_on_uncovered_test,
        )


def split_tests(
    requested_labels: str,
    root_dir: str,
    test_targets_json_path: str,
    raise_on_unused_label: bool = False,
    raise_on_uncovered_test: bool = False,
) -> None:
    labels = set(requested_labels.split(","))

    result = get_tests(
        requested_labels=labels,
        root_dir=pathlib.Path(root_dir),
        test_targets_json_path=pathlib.Path(test_targets_json_path),
        raise_on_unused_label=raise_on_unused_label,
        raise_on_uncovered_test=raise_on_uncovered_test,
    )
    print_output(labels, result)


cmd = functools.partial(clize.run, split_tests)

if __name__ == "__main__":
    cmd()
