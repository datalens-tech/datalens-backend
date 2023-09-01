import abc
import os
import shutil
import subprocess
from contextlib import contextmanager
from pathlib import Path
from typing import Callable, Generator, TextIO, Type, cast, final

import attr


@attr.s(frozen=True)
class FilesystemEditor(abc.ABC):
    base_path: Path = attr.ib(kw_only=True)

    def _validate_paths(self, *paths: Path) -> None:
        base_path_parts = self.base_path.absolute().parts
        for path in paths:
            path_parts = path.absolute().parts
            if path_parts[:len(base_path_parts)] != base_path_parts:
                raise RuntimeError(f'Access denied. Path is outside repository context: {path}')

    def _replace_file_content(self, file_path: Path, replace_callback: Callable[[str], str]) -> None:
        with self.open(file_path, 'r+') as f:
            old_text = f.read()
            new_text = replace_callback(old_text)
            f.seek(0)
            f.truncate(0)
            f.write(new_text)

    @final
    def replace_file_content(self, file_path: Path, replace_callback: Callable[[str], str]) -> None:
        self._validate_paths(file_path)
        self._replace_file_content(file_path=file_path, replace_callback=replace_callback)

    def _replace_text_in_file(self, file_path: Path, old_text: str, new_text: str) -> None:
        self.replace_file_content(
            file_path, replace_callback=lambda text: text.replace(old_text, new_text),
        )

    @final
    def replace_text_in_file(self, file_path: Path, old_text: str, new_text: str) -> None:
        self._validate_paths(file_path)
        self._replace_text_in_file(file_path=file_path, old_text=old_text, new_text=new_text)

    def _replace_text_in_dir(self, old_text: str, new_text: str, path: Path) -> None:
        for file_path in path.rglob("*/"):
            if file_path.is_file():
                self.replace_text_in_file(file_path, old_text=old_text, new_text=new_text)

    @final
    def replace_text_in_dir(self, old_text: str, new_text: str, path: Path) -> None:
        self._validate_paths(path)
        self._replace_text_in_dir(old_text=old_text, new_text=new_text, path=path)

    @abc.abstractmethod
    def _copy_path(self, src_dir: Path, dst_dir: Path) -> None:
        """Make a copy of `src_dir` named `dst_dir`."""
        raise NotImplementedError

    @final
    def copy_path(self, src_dir: Path, dst_dir: Path) -> None:
        self._validate_paths(src_dir, dst_dir)
        self._copy_path(src_dir=src_dir, dst_dir=dst_dir)

    @abc.abstractmethod
    def _move_path(self, old_path: Path, new_path: Path) -> None:
        raise NotImplementedError

    @final
    def move_path(self, old_path: Path, new_path: Path) -> None:
        self._validate_paths(old_path, new_path)
        self._move_path(old_path=old_path, new_path=new_path)

    @abc.abstractmethod
    def _remove_path(self, path: Path) -> None:
        raise NotImplementedError

    @final
    def remove_path(self, path: Path) -> None:
        self._validate_paths(path)
        self._remove_path(path=path)

    @final
    @contextmanager
    def open(self, path: Path, mode: str) -> Generator[TextIO, None, None]:
        # TODO: Make all toml editors open files via this method to enforce path restrictions
        assert mode in ('r', 'r+')
        self._validate_paths(path)
        with open(path, mode=mode) as file_obj:
            yield cast(TextIO, file_obj)


@attr.s(frozen=True)
class DefaultFilesystemEditor(FilesystemEditor):
    def _copy_path(self, src_dir: Path, dst_dir: Path) -> None:
        assert src_dir.exists(), 'Source dir doesn\'t exist'
        assert not dst_dir.exists(), 'Destination dir already exists'
        shutil.copytree(src_dir, dst_dir)

    def _move_path(self, old_path: Path, new_path: Path) -> None:
        assert old_path.exists()
        if old_path.is_dir():
            # module is a package
            print(f'Moving directory {old_path} to {new_path}')
            new_path.mkdir(exist_ok=True)

            for child in old_path.iterdir():
                new_child_path = new_path / child.name
                assert not new_child_path.exists(), f'Path {new_child_path} exists'
                shutil.move(child, new_path)

            shutil.rmtree(old_path)

        else:
            # module is a file
            assert old_path.is_file()
            assert not new_path.exists()

            print(f'Moving module {old_path} to {new_path}')
            new_path.parent.mkdir(exist_ok=True)

            shutil.move(old_path, new_path)

    def _remove_path(self, path: Path) -> None:
        shutil.rmtree(path)


@attr.s(frozen=True)
class GitFilesystemEditor(DefaultFilesystemEditor):
    """An FS editor that buses git to move files and directories"""

    def _move_path(self, old_path: Path, new_path: Path) -> None:
        cwd = Path.cwd()
        rel_old_path = Path(os.path.relpath(old_path, cwd))
        rel_new_path = Path(os.path.relpath(new_path, cwd))
        subprocess.run(f'git add "{rel_old_path}" && git mv "{rel_old_path}" "{rel_new_path}"', shell=True)

    def _remove_path(self, path: Path) -> None:
        subprocess.run(f'git rm "{path}"', shell=True)


_FS_EDITOR_CLASSES: dict[str, Type[FilesystemEditor]] = {
    'default': DefaultFilesystemEditor,
    'git': GitFilesystemEditor,
}


def get_fs_editor(fs_editor_type: str, base_path: Path) -> FilesystemEditor:
    fs_editor_cls = _FS_EDITOR_CLASSES[fs_editor_type]
    fs_editor = fs_editor_cls(base_path=base_path)
    return fs_editor
