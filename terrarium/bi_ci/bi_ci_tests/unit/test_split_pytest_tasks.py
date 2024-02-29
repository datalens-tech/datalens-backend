import contextlib
import dataclasses
import os
import shutil
import sys
import typing
import unittest.mock as mock
import uuid

import pytest
import pytest_mock

import bi_ci.split_pytest_tasks as split_pytest_tasks


class FolderContext(typing.Protocol):
    def __call__(self, path: typing.Optional[str] = None) -> typing.ContextManager[str]:
        ...


class FileContext(typing.Protocol):
    def __call__(
        self, path: typing.Optional[str] = None, data: typing.Optional[str] = None
    ) -> typing.ContextManager[str]:
        ...


@pytest.fixture(name="folder_context")
def fixture_folder_context() -> FolderContext:
    @contextlib.contextmanager
    def context(path: typing.Optional[str] = None) -> typing.ContextManager[str]:
        if path is None:
            path = f"/tmp/{uuid.uuid4()}"

        os.makedirs(path)

        try:
            yield path
        finally:
            shutil.rmtree(path, ignore_errors=True)

    return context


@pytest.fixture(name="file_context")
def fixture_file_context() -> FileContext:
    @contextlib.contextmanager
    def context(path: typing.Optional[str] = None, data: typing.Optional[str] = None) -> typing.ContextManager[str]:
        if path is None:
            path = f"/tmp/{uuid.uuid4()}"

        os.makedirs(os.path.dirname(path), exist_ok=True)

        with open(path, "w") as file:
            if data is not None:
                file.write(data)

        try:
            yield path
        finally:
            try:
                os.remove(path)
            except FileNotFoundError:
                pass

    return context


TestTargetsJsonContext = typing.Callable[[list[str]], typing.ContextManager[str]]


@pytest.fixture(name="mocked_print")
def fixture_mocked_print(mocker: pytest_mock.MockerFixture) -> mock.Mock:
    return mocker.patch("builtins.print")


@pytest.fixture(name="test_targets_json_context")
def fixture_test_targets_json_context(file_context: FileContext) -> TestTargetsJsonContext:
    @contextlib.contextmanager
    def context(data: list[str]) -> typing.ContextManager[str]:
        with file_context(data=str(data)) as path:
            yield path

    return context


@dataclasses.dataclass
class LibContext:
    pyproject: str = ""
    pytest_files: list[str] = dataclasses.field(default_factory=list)


class LibsPyprojectsContext(typing.Protocol):
    def __call__(self, libs: dict[str, LibContext]) -> typing.ContextManager[str]:
        ...


@pytest.fixture(name="libs_pyprojects_context")
def fixture_libs_pyprojects_context(folder_context: FolderContext, file_context: FileContext) -> LibsPyprojectsContext:
    @contextlib.contextmanager
    def context(libs: dict[str, LibContext]) -> typing.ContextManager[str]:
        with folder_context() as root_path:
            with contextlib.ExitStack() as lib_contexts:
                for lib_name, lib_context in libs.items():
                    lib_contexts.enter_context(folder_context(path=f"{root_path}/{lib_name}"))
                    lib_contexts.enter_context(
                        file_context(path=f"{root_path}/{lib_name}/pyproject.toml", data=lib_context.pyproject)
                    )
                    for pytest_file in lib_context.pytest_files:
                        lib_contexts.enter_context(file_context(path=f"{root_path}/{lib_name}/{pytest_file}"))

                yield root_path

    return context


@pytest.mark.parametrize("requested_mode", ["base", "not_base"])
def test_split_tests_no_pyproject(
    mocked_print: mock.Mock,
    test_targets_json_context: TestTargetsJsonContext,
    libs_pyprojects_context: LibsPyprojectsContext,
    requested_mode: str,
):
    with test_targets_json_context(["package"]) as test_targets_json_path, libs_pyprojects_context({}) as root_path:
        split_pytest_tasks.split_tests(requested_mode, root_path, test_targets_json_path)

    mocked_print.assert_has_calls(
        [
            mock.call(mock.ANY, file=sys.stderr),
            mock.call(f"split_{requested_mode}=[]"),
        ]
    )


@pytest.mark.parametrize(
    "requested_mode, result",
    [
        ("base", '["package:__default__"]'),
        ("not_base", "[]"),
    ],
)
def test_split_tests_without_targets(
    mocked_print: mock.Mock,
    test_targets_json_context: TestTargetsJsonContext,
    libs_pyprojects_context: LibsPyprojectsContext,
    requested_mode: str,
    result: str,
):
    with test_targets_json_context(["package"]) as test_targets_json_path, libs_pyprojects_context(
        {"package": LibContext()}
    ) as root_path:
        split_pytest_tasks.split_tests(requested_mode, root_path, test_targets_json_path)

    mocked_print.assert_has_calls([mock.call(f"split_{requested_mode}={result}")])


@pytest.mark.parametrize(
    "requested_mode, result",
    [
        ("base", '["package:unit"]'),
        ("not_base", "[]"),
    ],
)
def test_split_tests_without_label(
    mocked_print: mock.Mock,
    test_targets_json_context: TestTargetsJsonContext,
    libs_pyprojects_context: LibsPyprojectsContext,
    requested_mode: str,
    result: str,
):
    pyproject = """
    [datalens.pytest.unit]
    root_dir = "tests"
    """
    with test_targets_json_context(["package"]) as test_targets_json_path, libs_pyprojects_context(
        {"package": LibContext(pyproject=pyproject)}
    ) as root_path:
        split_pytest_tasks.split_tests(requested_mode, root_path, test_targets_json_path)

    mocked_print.assert_has_calls([mock.call(f"split_{requested_mode}={result}")])


@pytest.mark.parametrize("requested_mode", ["base", "not_base"])
def test_split_tests_with_label(
    mocked_print: mock.Mock,
    test_targets_json_context: TestTargetsJsonContext,
    libs_pyprojects_context: LibsPyprojectsContext,
    requested_mode: str,
):
    pyproject = f"""
    [datalens.pytest.unit]
    root_dir = "tests"
    labels = ["{requested_mode}"]
    """

    with test_targets_json_context(["package"]) as test_targets_json_path, libs_pyprojects_context(
        {"package": LibContext(pyproject=pyproject)}
    ) as root_path:
        split_pytest_tasks.split_tests(requested_mode, root_path, test_targets_json_path)

    mocked_print.assert_has_calls([mock.call(f'split_{requested_mode}=["package:unit"]')])


def test_split_tests_multiple(
    mocked_print: mock.Mock,
    test_targets_json_context: TestTargetsJsonContext,
    libs_pyprojects_context: LibsPyprojectsContext,
):
    a_pyproject = """
    [datalens.pytest.unit]
    root_dir = "tests"
    labels = ["base"]
    
    [datalens.pytest.integration]
    root_dir = "tests"
    
    [datalens.pytest.e2e]
    root_dir = "tests"
    labels = ["fat"]
    """
    b_pyproject = """
    [datalens.pytest.unit]
    root_dir = "tests"
    labels = ["base"]
    
    [datalens.pytest.integration]
    root_dir = "tests"
    labels = ["fat"]
    
    [datalens.pytest.e2e]
    root_dir = "tests"
    """

    with test_targets_json_context(["a", "b"]) as test_targets_json_path, libs_pyprojects_context(
        {"a": LibContext(pyproject=a_pyproject), "b": LibContext(pyproject=b_pyproject)}
    ) as root_path:
        split_pytest_tasks.split_tests("base,fat,ext", root_path, test_targets_json_path)

    mocked_print.assert_has_calls(
        [
            mock.call('split_base=["a:unit", "a:integration", "b:unit", "b:e2e"]'),
            mock.call('split_fat=["a:e2e", "b:integration"]'),
            mock.call("split_ext=[]"),
        ],
        any_order=True,
    )


def test_split_tests_nested(
    mocked_print: mock.Mock,
    test_targets_json_context: TestTargetsJsonContext,
    libs_pyprojects_context: LibsPyprojectsContext,
):
    with test_targets_json_context(["lib/package"]) as test_targets_json_path, libs_pyprojects_context(
        {"lib/package": LibContext()}
    ) as root_path:
        split_pytest_tasks.split_tests("base", root_path, test_targets_json_path)

    mocked_print.assert_has_calls([mock.call('split_base=["lib/package:__default__"]')])


def test_split_tests_raise_on_unused_label(
    mocked_print: mock.Mock,
    test_targets_json_context: TestTargetsJsonContext,
    libs_pyprojects_context: LibsPyprojectsContext,
):
    pyproject = """
    [datalens.pytest.unit]
    root_dir = "tests"
    labels = ["unused"]
    """

    with test_targets_json_context(["package"]) as test_targets_json_path, libs_pyprojects_context(
        {"package": LibContext(pyproject=pyproject)}
    ) as root_path:
        with pytest.raises(ValueError):
            split_pytest_tasks.split_tests(
                "base,not_base",
                root_path,
                test_targets_json_path,
                raise_on_unused_label=True,
            )

    mocked_print.assert_has_calls([mock.call("Unused labels in package:unit: unused", file=sys.stderr)])


def test_split_tests_not_raise_on_unused_label_if_no_flag(
    mocked_print: mock.Mock,
    test_targets_json_context: TestTargetsJsonContext,
    libs_pyprojects_context: LibsPyprojectsContext,
):
    pyproject = """
    [datalens.pytest.unit]
    root_dir = "tests"
    labels = ["unused"]
    """

    with test_targets_json_context(["package"]) as test_targets_json_path, libs_pyprojects_context(
        {"package": LibContext(pyproject=pyproject)}
    ) as root_path:
        split_pytest_tasks.split_tests("base,not_base", root_path, test_targets_json_path)

    mocked_print.assert_has_calls(
        [
            mock.call("Unused labels in package:unit: unused", file=sys.stderr),
            mock.call("split_base=[]"),
            mock.call("split_not_base=[]"),
        ],
        any_order=True,
    )


@pytest.mark.parametrize(
    "pyproject",
    [
        """
        [datalens.pytest.unit]
        root_dir = "package_tests"
        target_path = "unit"
        """,
        """
        [datalens.pytest.unit]
        root_dir = "package_tests/unit"
        """,
    ],
    ids=["target_path", "root_dir"],
)
def test_split_tests_raise_on_uncovered_test(
    mocked_print: mock.Mock,
    test_targets_json_context: TestTargetsJsonContext,
    libs_pyprojects_context: LibsPyprojectsContext,
    pyproject: str,
):
    with test_targets_json_context(["package"]) as test_targets_json_path, libs_pyprojects_context(
        {"package": LibContext(pyproject=pyproject, pytest_files=["package_tests/not_unit/test_random.py"])}
    ) as root_path:
        with pytest.raises(ValueError):
            split_pytest_tasks.split_tests(
                "base",
                root_path,
                test_targets_json_path,
                raise_on_uncovered_test=True,
            )

    mocked_print.assert_has_calls(
        [
            mock.call(
                f"Uncovered test file {root_path}/package/package_tests/not_unit/test_random.py found in package",
                file=sys.stderr,
            ),
        ],
    )


def test_split_tests_absent_target_path_covers_all_root_dir(
    mocked_print: mock.Mock,
    test_targets_json_context: TestTargetsJsonContext,
    libs_pyprojects_context: LibsPyprojectsContext,
):
    pyproject = """
    [datalens.pytest.unit]
    root_dir = "tests"
    """

    with test_targets_json_context(["package"]) as test_targets_json_path, libs_pyprojects_context(
        {"package": LibContext(pyproject=pyproject, pytest_files=["tests/not_unit/test_random.py"])}
    ) as root_path:
        split_pytest_tasks.split_tests("base", root_path, test_targets_json_path, raise_on_uncovered_test=True)

    mocked_print.assert_has_calls([mock.call('split_base=["package:unit"]')])


def test_split_tests_absent_target_covers_default_tests_folder(
    mocked_print: mock.Mock,
    test_targets_json_context: TestTargetsJsonContext,
    libs_pyprojects_context: LibsPyprojectsContext,
):
    with test_targets_json_context(["package"]) as test_targets_json_path, libs_pyprojects_context(
        {"package": LibContext(pyproject="", pytest_files=["package_tests/test_random.py"])}
    ) as root_path:
        split_pytest_tasks.split_tests("base", root_path, test_targets_json_path, raise_on_uncovered_test=True)

    mocked_print.assert_has_calls([mock.call('split_base=["package:__default__"]')])
