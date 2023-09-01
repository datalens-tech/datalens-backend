import abc
import os
import shutil
import subprocess
from pathlib import Path
from typing import Callable


class FilesystemEditor(abc.ABC):
    @staticmethod
    def replace_file_content(file_path: Path, replace_callback: Callable[[str], str]) -> None:
        with open(file_path, 'r+') as f:
            old_text = f.read()
            new_text = replace_callback(old_text)
            f.seek(0)
            f.truncate(0)
            f.write(new_text)

    @classmethod
    def replace_text_in_file(cls, file_path: Path, old_text: str, new_text: str) -> None:
        cls.replace_file_content(
            file_path, replace_callback=lambda text: text.replace(old_text, new_text),
        )

    @abc.abstractmethod
    def copy_path(self, src_dir: Path, dst_dir: Path) -> None:
        """Make a copy of `src_dir` named `dst_dir`."""
        raise NotImplementedError

    @classmethod
    def replace_text_in_dir(cls, old_text: str, new_text: str, path: Path) -> None:
        for file_path in path.rglob("*/"):
            if file_path.is_file():
                cls.replace_text_in_file(file_path, old_text=old_text, new_text=new_text)

    @abc.abstractmethod
    def move_path(self, old_path: Path, new_path: Path) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def remove_path(self, path: Path) -> None:
        raise NotImplementedError


class DefaultFilesystemEditor(FilesystemEditor):
    def copy_path(self, src_dir: Path, dst_dir: Path) -> None:
        assert src_dir.exists(), 'Source dir doesn\'t exist'
        assert not dst_dir.exists(), 'Destination dir already exists'
        shutil.copytree(src_dir, dst_dir)

    def move_path(self, old_path: Path, new_path: Path) -> None:
        assert old_path.exists()
        if old_path.is_dir():
            # module is a package
            print(f'Moving directory {old_path} to {new_path}')
            new_path.mkdir(exist_ok=True)

            for name in old_path.iterdir():
                assert not (new_path / name).exists()
                shutil.move(old_path / name, new_path)

            shutil.rmtree(old_path)

        else:
            # module is a file
            assert old_path.is_file()
            assert not new_path.exists()

            print(f'Moving module {old_path} to {new_path}')
            new_path.parent.mkdir(exist_ok=True)

            shutil.move(old_path, new_path)

    def remove_path(self, path: Path) -> None:
        shutil.rmtree(path)


class GitFilesystemEditor(DefaultFilesystemEditor):
    """An FS editor that buses git to move files and directories"""

    def move_path(self, old_path: Path, new_path: Path) -> None:
        cwd = Path.cwd()
        rel_old_path = Path(os.path.relpath(old_path, cwd))
        rel_new_path = Path(os.path.relpath(new_path, cwd))
        subprocess.run(f'git add "{rel_old_path}" && git mv "{rel_old_path}" "{rel_new_path}"', shell=True)

    def remove_path(self, path: Path) -> None:
        subprocess.run(f'git rm "{path}"', shell=True)
