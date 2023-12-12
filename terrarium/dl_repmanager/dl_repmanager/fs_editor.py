import abc
from contextlib import contextmanager
import io
import os
from pathlib import Path
import re
import shutil
import subprocess
from typing import (
    Callable,
    Collection,
    Generator,
    TextIO,
    Type,
    cast,
    final,
)

import attr
from git import Repo as GitRepo


@attr.s(frozen=True)
class FilesystemEditor(abc.ABC):
    base_path: Path = attr.ib(kw_only=True)

    def _is_subpath(self, root: Path, child: Path) -> bool:
        root_parts = root.absolute().parts
        child_parts = child.absolute().parts
        return child_parts[: len(root_parts)] == root_parts

    def _validate_paths(self, *paths: Path) -> None:
        base_path_parts = self.base_path.absolute().parts
        for path in paths:
            path_parts = path.absolute().parts
            if path_parts[: len(base_path_parts)] != base_path_parts:
                raise RuntimeError(f"Access denied. Path is outside repository context: {path}")

    def _replace_file_content(self, file_path: Path, replace_callback: Callable[[str], str]) -> None:
        with self.open(file_path, "r+") as f:
            old_text = f.read()
            new_text = replace_callback(old_text)
            if new_text != old_text:
                f.seek(0)
                f.truncate(0)
                f.write(new_text)

    @final
    def replace_file_content(self, file_path: Path, replace_callback: Callable[[str], str]) -> None:
        self._validate_paths(file_path)
        self._replace_file_content(file_path=file_path, replace_callback=replace_callback)

    def _replace_text_in_file(self, file_path: Path, old_text: str, new_text: str) -> None:
        self.replace_file_content(
            file_path,
            replace_callback=lambda text: text.replace(old_text, new_text),
        )

    @final
    def replace_text_in_file(self, file_path: Path, old_text: str, new_text: str) -> None:
        self._validate_paths(file_path)
        self._replace_text_in_file(file_path=file_path, old_text=old_text, new_text=new_text)

    def _replace_regex_in_file(
        self,
        file_path: Path,
        regex: re.Pattern,
        repl: Callable[[re.Match], str],
    ) -> None:
        self.replace_file_content(
            file_path,
            replace_callback=lambda text: regex.sub(repl, text),
        )

    @final
    def replace_regex_in_file(self, file_path: Path, regex: re.Pattern, repl: Callable[[re.Match], str]) -> None:
        self._validate_paths(file_path)
        self._replace_regex_in_file(file_path=file_path, regex=regex, repl=repl)

    def _iter_files(
        self,
        path: Path,
        exclude_masks: Collection[re.Pattern] = (),
    ) -> Generator[Path, None, None]:
        for file_path in path.rglob("*"):
            if file_path.is_file():
                matches_exclude_mask = False
                for mask in exclude_masks:
                    if mask.match(str(file_path)):
                        matches_exclude_mask = True
                if matches_exclude_mask:
                    continue

                yield file_path

    def _replace_text_in_dir(
        self,
        old_text: str,
        new_text: str,
        path: Path,
        exclude_masks: Collection[re.Pattern] = (),
    ) -> None:
        for file_path in self._iter_files(path=path, exclude_masks=exclude_masks):
            self.replace_text_in_file(file_path, old_text=old_text, new_text=new_text)

    @final
    def replace_text_in_dir(
        self,
        old_text: str,
        new_text: str,
        path: Path,
        exclude_masks: Collection[re.Pattern] = (),
    ) -> None:
        self._validate_paths(path)
        self._replace_text_in_dir(old_text=old_text, new_text=new_text, path=path, exclude_masks=exclude_masks)

    def _replace_regex_in_dir(
        self,
        path: Path,
        regex: re.Pattern,
        repl: Callable[[re.Match], str],
        exclude_masks: Collection[re.Pattern] = (),
    ) -> None:
        for file_path in self._iter_files(path=path, exclude_masks=exclude_masks):
            self.replace_regex_in_file(file_path, regex=regex, repl=repl)

    @final
    def replace_regex_in_dir(
        self,
        path: Path,
        regex: re.Pattern,
        repl: Callable[[re.Match], str],
        exclude_masks: Collection[re.Pattern] = (),
    ) -> None:
        self._validate_paths(path)
        self._replace_regex_in_dir(path=path, regex=regex, repl=repl, exclude_masks=exclude_masks)

    @abc.abstractmethod
    def _copy_path(self, src_path: Path, dst_path: Path) -> None:
        """Make a copy of `src_dir` named `dst_dir`."""
        raise NotImplementedError

    @final
    def copy_path(self, src_path: Path, dst_path: Path) -> None:
        self._validate_paths(src_path, dst_path)
        self._copy_path(src_path=src_path, dst_path=dst_path)

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

    @abc.abstractmethod
    def _path_exists(self, path: Path) -> bool:
        raise NotImplementedError

    @final
    def path_exists(self, path: Path) -> bool:
        self._validate_paths(path)
        return self._path_exists(path=path)

    @contextmanager
    def _open(self, path: Path, mode: str) -> Generator[TextIO, None, None]:
        with open(path, mode=mode) as file_obj:
            yield cast(TextIO, file_obj)

    @final
    @contextmanager
    def open(self, path: Path, mode: str) -> Generator[TextIO, None, None]:
        # TODO: Make all toml editors open files via this method to enforce path restrictions
        assert mode in ("r", "r+", "w")
        self._validate_paths(path)
        with self._open(path, mode=mode) as file_obj:
            yield file_obj


@attr.s(frozen=True)
class DefaultFilesystemEditor(FilesystemEditor):
    def _copy_path(self, src_path: Path, dst_path: Path) -> None:
        assert src_path.exists(), f"Source path '{src_path}' doesn't exist"
        assert not dst_path.exists(), "Destination dir already exists"
        if src_path.is_file():
            shutil.copy(src_path, dst_path)
        else:
            shutil.copytree(src_path, dst_path)

    def _move_path(self, old_path: Path, new_path: Path) -> None:
        assert old_path.exists()
        if old_path.is_dir():
            # module is a package
            print(f"Moving directory {old_path} to {new_path}")
            new_path.mkdir(exist_ok=True)

            for child in old_path.iterdir():
                new_child_path = new_path / child.name
                assert not new_child_path.exists(), f"Path {new_child_path} exists"
                shutil.move(child, new_path)

            shutil.rmtree(old_path)

        else:
            # module is a file
            assert old_path.is_file()
            assert not new_path.exists()

            print(f"Moving module {old_path} to {new_path}")
            new_path.parent.mkdir(exist_ok=True)

            shutil.move(old_path, new_path)

    def _remove_path(self, path: Path) -> None:
        if path.is_file():
            os.remove(path)
        else:
            shutil.rmtree(path)

    def _path_exists(self, path: Path) -> bool:
        return path.exists()


@attr.s(frozen=True)
class GitFilesystemEditor(DefaultFilesystemEditor):
    """An FS editor that buses git to move files and directories"""

    def _validate_paths_in_same_repo(self, *paths: Path) -> None:
        root_paths = {GitRepo(path, search_parent_directories=True).working_tree_dir for path in paths}
        if len(root_paths) > 1:
            raise RuntimeError(
                "Cross-repository operations are not supported for GitFilesystemEditor. Use `--fs-editor default`"
            )

    def _find_existing_parent(self, path: Path) -> Path:
        assert path.is_absolute()
        while not path.exists():
            if path.parent == path:
                break
            path = path.parent
        return path

    def _move_path(self, old_path: Path, new_path: Path) -> None:
        self._validate_paths_in_same_repo(old_path, self._find_existing_parent(new_path))
        cwd = Path.cwd()
        rel_old_path = Path(os.path.relpath(old_path, cwd))
        rel_new_path = Path(os.path.relpath(new_path, cwd))
        subprocess.run(f'git add "{rel_old_path}" && git mv "{rel_old_path}" "{rel_new_path}"', shell=True)

    def _remove_path(self, path: Path) -> None:
        result = subprocess.run(f'git rm -r "{path}"', shell=True)
        if result.returncode != 0:
            super()._remove_path(path)


@attr.s(frozen=True)
class VirtualFilesystemEditor(FilesystemEditor):
    _moved_paths: dict[Path, Path] = attr.ib(init=False, factory=dict)  # {<new_path>: <old_path>}
    _copied_paths: dict[Path, Path] = attr.ib(init=False, factory=dict)  # {<new_path>: <original_path>}
    _removed_paths: set[Path] = attr.ib(init=False, factory=set)
    _edited_files: dict[Path, str] = attr.ib(init=False, factory=dict)  # {<current_path>: <content>}

    def ensure_path_exists(self, path: Path) -> None:
        if not path.exists():
            raise RuntimeError(f"Path does not exist: {path}")

    def ensure_path_doesnt_exist(self, path: Path) -> None:
        if path.exists():
            raise RuntimeError(f"Path already exists: {path}")

    def _recursive_children(self, path: Path) -> list[Path]:
        children: set[Path] = set()
        if path.is_file():
            return [path]
        for real_child in path.rglob("*/"):
            if real_child in self._moved_paths.values():
                # It has been moved
                continue
            if real_child in self._removed_paths:
                # It has been deleted
                continue

            children.add(real_child)

        for virtual_child in set(self._moved_paths) | set(self._copied_paths):
            if self._is_subpath(root=path, child=virtual_child):
                continue
            children.add(virtual_child)

        return sorted(children)

    def _copy_single_path(self, src_path: Path, dst_path: Path) -> None:
        self.ensure_path_exists(src_path)
        self.ensure_path_doesnt_exist(dst_path)

        if src_path in self._moved_paths:
            self._copied_paths[dst_path] = self._moved_paths[src_path]
        elif src_path in self._copied_paths:
            self._copied_paths[dst_path] = self._copied_paths[src_path]

        if src_path in self._edited_files:
            self._edited_files[dst_path] = self._edited_files[src_path]

    def _copy_path(self, src_path: Path, dst_path: Path) -> None:
        self._copy_single_path(src_path=src_path, dst_path=dst_path)
        for src_child_path in self._recursive_children(src_path):
            dst_child_path = dst_path / src_child_path.relative_to(src_path)
            self._copy_single_path(src_path=src_child_path, dst_path=dst_child_path)

    def _move_single_path(self, src_path: Path, dst_path: Path) -> None:
        self.ensure_path_exists(src_path)
        self.ensure_path_doesnt_exist(dst_path)

        if src_path in self._moved_paths:
            original_path = self._moved_paths[src_path]
            del self._moved_paths[src_path]
            self._moved_paths[dst_path] = original_path
        elif src_path in self._copied_paths:
            original_path = self._copied_paths[src_path]
            del self._copied_paths[src_path]
            self._copied_paths[dst_path] = original_path

        if src_path in self._edited_files:
            self._edited_files[dst_path] = self._edited_files.pop(src_path)

    def _move_path(self, old_path: Path, new_path: Path) -> None:
        self._move_single_path(src_path=old_path, dst_path=new_path)
        for src_child_path in self._recursive_children(old_path):
            dst_child_path = new_path / src_child_path.relative_to(old_path)
            self._move_single_path(src_path=src_child_path, dst_path=dst_child_path)

    def _remove_path(self, path: Path) -> None:
        if path in self._moved_paths:
            original_path = self._moved_paths[path]
            del self._moved_paths[path]
            self._removed_paths.add(original_path)
        elif path in self._copied_paths:
            del self._copied_paths[path]
        else:
            self._removed_paths.add(path)

        if path in self._edited_files:
            del self._edited_files[path]

    @contextmanager
    def _open(self, path: Path, mode: str) -> Generator[TextIO, None, None]:
        original_path = path
        if path in self._moved_paths:
            original_path = self._moved_paths[path]
        elif path in self._copied_paths:
            original_path = self._copied_paths[path]

        if path in self._edited_files:
            original_text = self._edited_files[path]
        elif mode == "w" and not original_path.exists():
            original_text = ""
        else:
            with super()._open(original_path, mode="r") as file_obj:
                original_text = file_obj.read()

        str_io: io.StringIO
        if mode == "w":
            str_io = io.StringIO()
        else:
            str_io = io.StringIO(original_text)
        yield str_io

        if mode in ("r+", "w"):
            new_text = str_io.getvalue()
            if new_text != original_text:
                self._edited_files[path] = new_text

    def _path_exists(self, path: Path) -> bool:
        return (
            path in self._moved_paths or path in self._copied_paths or path in self._edited_files or path.exists()
        ) and path not in self._removed_paths


_FS_EDITOR_CLASSES: dict[str, Type[FilesystemEditor]] = {
    "default": DefaultFilesystemEditor,
    "git": GitFilesystemEditor,
    "virtual": VirtualFilesystemEditor,
}


def get_fs_editor(fs_editor_type: str, base_path: Path) -> FilesystemEditor:
    fs_editor_cls = _FS_EDITOR_CLASSES[fs_editor_type]
    fs_editor = fs_editor_cls(base_path=base_path)
    return fs_editor
