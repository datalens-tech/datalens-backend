import contextlib
import os
import shutil
import sys
import typing
import unittest.mock as mock
import uuid

import pytest
import pytest_mock

import bi_ci.split_pytest_tasks as split_pytest_tasks


TestTargetsJsonContext = typing.Callable[[list[str]], typing.ContextManager[str]]
LibsPyprojectsContext = typing.Callable[[dict[str, str]], typing.ContextManager[str]]


@pytest.fixture(name="mocked_print")
def fixture_mocked_print(mocker: pytest_mock.MockerFixture) -> mock.Mock:
    return mocker.patch("builtins.print")


@pytest.fixture(name="test_targets_json_context")
def fixture_test_targets_json_context() -> TestTargetsJsonContext:
    @contextlib.contextmanager
    def context(data: list[str]) -> typing.ContextManager[str]:
        path = f"/tmp/{uuid.uuid4()}.json"

        with open(path, "w") as file:
            file.write(str(data))

        try:
            yield path
        finally:
            try:
                os.remove(path)
            except FileNotFoundError:
                pass

    return context


@pytest.fixture(name="libs_pyprojects_context")
def fixture_libs_pyprojects_context() -> LibsPyprojectsContext:
    @contextlib.contextmanager
    def context(data: dict[str, str]) -> typing.ContextManager[str]:
        root_path = f"/tmp/{uuid.uuid4()}"
        os.makedirs(root_path)

        for package_name, pyproject_data in data.items():
            package_path = f"{root_path}/{package_name}"
            os.makedirs(package_path)

            with open(f"{package_path}/pyproject.toml", "w") as file:
                file.write(pyproject_data)

        try:
            yield root_path
        finally:
            shutil.rmtree(root_path, ignore_errors=True)

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
        {"package": ""}
    ) as root_path:
        split_pytest_tasks.split_tests(requested_mode, root_path, test_targets_json_path)

    mocked_print.assert_called_once_with(f"split_{requested_mode}={result}")


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
    pyproject_data = """
    [datalens.pytest.unit]
    """
    with test_targets_json_context(["package"]) as test_targets_json_path, libs_pyprojects_context(
        {"package": pyproject_data}
    ) as root_path:
        split_pytest_tasks.split_tests(requested_mode, root_path, test_targets_json_path)

    mocked_print.assert_called_once_with(f"split_{requested_mode}={result}")


@pytest.mark.parametrize("requested_mode", ["base", "not_base"])
def test_split_tests_with_label(
    mocked_print: mock.Mock,
    test_targets_json_context: TestTargetsJsonContext,
    libs_pyprojects_context: LibsPyprojectsContext,
    requested_mode: str,
):
    pyproject_data = f"""
    [datalens.pytest.unit]
    labels = ["{requested_mode}"]
    """

    with test_targets_json_context(["package"]) as test_targets_json_path, libs_pyprojects_context(
        {"package": pyproject_data}
    ) as root_path:
        split_pytest_tasks.split_tests(requested_mode, root_path, test_targets_json_path)

    mocked_print.assert_called_once_with(f'split_{requested_mode}=["package:unit"]')


def test_split_tests_multiple(
    mocked_print: mock.Mock,
    test_targets_json_context: TestTargetsJsonContext,
    libs_pyprojects_context: LibsPyprojectsContext,
):
    a_pyproject_data = """
    [datalens.pytest.unit]
    labels = ["base"]
    
    [datalens.pytest.integration]
    
    [datalens.pytest.e2e]
    labels = ["fat"]
    """
    b_project_data = """
    [datalens.pytest.unit]
    labels = ["base"]
    
    [datalens.pytest.integration]
    labels = ["fat"]
    
    [datalens.pytest.e2e]
    """

    with test_targets_json_context(["a", "b"]) as test_targets_json_path, libs_pyprojects_context(
        {"a": a_pyproject_data, "b": b_project_data}
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
