from pathlib import Path
import shutil
import tempfile
from typing import Generator

import attr
import pytest

from dl_repmanager.fs_editor import (
    DefaultFilesystemEditor,
    FilesystemEditor,
    VirtualFilesystemEditor,
)


@attr.s
class FSSetup:
    base_path: Path = attr.ib(kw_only=True)


class FSEditorTestCase:
    @pytest.fixture(scope="class")
    def base_setup(self) -> Generator[FSSetup, None, None]:
        base_path = Path(tempfile.mkdtemp())
        try:
            yield FSSetup(base_path=base_path)
        except Exception:
            shutil.rmtree(base_path)

    @pytest.fixture(scope="class")
    def fs_editor(self, base_setup: FSSetup) -> FilesystemEditor:
        raise NotImplementedError

    def test_file_access(self, base_setup: FSSetup, fs_editor: FilesystemEditor) -> None:
        testfile_path = base_setup.base_path / "testfile.thing"
        print(base_setup.base_path)
        with fs_editor.open(testfile_path, mode="w") as testfile:
            testfile.write("qwerty")

        with fs_editor.open(testfile_path, mode="r") as testfile:
            text = testfile.read()

        assert text == "qwerty"


class TestVirtualFSEditor(FSEditorTestCase):
    @pytest.fixture(scope="class")
    def fs_editor(self, base_setup: FSSetup) -> FilesystemEditor:
        return VirtualFilesystemEditor(base_path=base_setup.base_path)


class TestDefaultFSEditor(FSEditorTestCase):
    @pytest.fixture(scope="class")
    def fs_editor(self, base_setup: FSSetup) -> FilesystemEditor:
        return DefaultFilesystemEditor(base_path=base_setup.base_path)
