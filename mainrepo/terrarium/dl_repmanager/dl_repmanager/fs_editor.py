import abc
import os
import shutil
import subprocess
from typing import Callable


class FilesystemEditor(abc.ABC):
    @staticmethod
    def replace_file_content(file_path: str, replace_callback: Callable[[str], str]) -> None:
        with open(file_path, 'r+') as f:
            old_text = f.read()
            new_text = replace_callback(old_text)
            f.seek(0)
            f.truncate(0)
            f.write(new_text)

    @classmethod
    def replace_text_in_file(cls, file_path: str, old_text: str, new_text: str) -> None:
        cls.replace_file_content(
            file_path, replace_callback=lambda text: text.replace(old_text, new_text),
        )

    @abc.abstractmethod
    def copy_dir(self, src_dir: str, dst_dir: str) -> None:
        """Make a copy of `src_dir` named `dst_dir`."""
        raise NotImplementedError

    @classmethod
    def replace_text_in_dir(cls, old_text: str, new_text: str, path: str) -> None:
        for root, dirs, files in os.walk(path):
            for file_name in files:
                file_path = os.path.join(root, file_name)
                cls.replace_text_in_file(file_path, old_text=old_text, new_text=new_text)

    @abc.abstractmethod
    def move_path(self, old_path: str, new_path: str) -> None:
        raise NotImplementedError


class DefaultFilesystemEditor(FilesystemEditor):
    def copy_dir(self, src_dir: str, dst_dir: str) -> None:
        assert os.path.exists(src_dir), 'Source dir doesn\'t exist'
        assert not os.path.exists(dst_dir), 'Destination dir already exists'
        shutil.copytree(src_dir, dst_dir)

    def move_path(self, old_path: str, new_path: str) -> None:
        assert os.path.exists(old_path)
        if os.path.isdir(old_path):
            # module is a package
            print(f'Moving directory {old_path} to {new_path}')
            if not os.path.exists(new_path):
                os.makedirs(new_path)

            for name in os.listdir(old_path):
                assert not os.path.exists(os.path.join(new_path, name))
                shutil.move(os.path.join(old_path, name), new_path)

            os.rmdir(old_path)

        else:
            # module is a file
            assert os.path.isfile(old_path)
            assert not os.path.exists(new_path)
            print(f'Moving module {old_path} to {new_path}')
            new_dir = os.path.dirname(new_path)
            if not os.path.exists(new_dir):
                os.makedirs(new_dir)

            shutil.move(old_path, new_path)


class GitFilesystemEditor(DefaultFilesystemEditor):
    """An FS editor that buses git to move files and directories"""

    def move_path(self, old_path: str, new_path: str) -> None:
        cwd = os.getcwd()
        rel_old_path = os.path.relpath(old_path, cwd)
        rel_new_path = os.path.relpath(new_path, cwd)
        subprocess.run(f'git add "{rel_old_path}" && git mv "{rel_old_path}" "{rel_new_path}"', shell=True)
