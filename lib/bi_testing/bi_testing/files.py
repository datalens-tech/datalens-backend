import abc
import contextlib
from typing import Any, Generator

_is_arcadia: bool = False
yatest_common: Any
try:
    import yatest.common as yatest_common
    _is_arcadia = True

except ImportError:
    yatest_common = None


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


class ArcadiaFileLoader(FileLoader):  # TODO: Remove once we're out of arcadia
    def resolve_path(self, path: str) -> str:
        assert yatest_common is not None
        # Make sure that the path is valid
        repo_prefix = 'datalens/backend/'
        assert path.startswith(repo_prefix), f'Path doesn\'t start with repo prefix: {path}"'
        # Transform it for usage in arcadia
        path = yatest_common.source_path(path)  # type: ignore
        return path


def get_file_loader() -> FileLoader:
    file_loader: FileLoader
    if _is_arcadia:
        file_loader = ArcadiaFileLoader()
    else:
        file_loader = DefaultFileLoader()

    return file_loader
