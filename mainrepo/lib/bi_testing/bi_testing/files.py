import abc
import contextlib
from typing import Any, Generator


class FileLoader(abc.ABC):
    """
    The path passed to `FileLoader` must be built
    via the `__file__` module variable/attribute
    for it to work in all environments.
    """

    @abc.abstractmethod
    def resolve_path(self, path: str) -> str:
        raise NotImplementedError

    @contextlib.contextmanager
    def open(self, filename, mode: str = 'r') -> Generator[Any, None, None]:
        path = self.resolve_path(filename)
        with open(path, mode=mode) as f:
            yield f


class DefaultFileLoader(FileLoader):
    def resolve_path(self, path: str) -> str:
        # Just leave the path as is
        return path


def get_file_loader() -> FileLoader:
    return DefaultFileLoader()
